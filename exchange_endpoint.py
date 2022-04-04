from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)


@app.before_request
def create_session():
    g.session = scoped_session(DBSession)


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """


def check_sig(payload, sig):
    pass


def fill_order(order, txes=[]):
    order_obj = Order(sender_pk=order['sender_pk'], receiver_pk=order['receiver_pk'],
                      buy_currency=order['buy_currency'], sell_currency=order['sell_currency'],
                      buy_amount=order['buy_amount'], sell_amount=order['sell_amount'],
                      creator_id=order.get('creator_id'))

    result = g.session.query(Order).filter(Order.filled == None, Order.buy_currency == order['sell_currency'],
                                         Order.sell_currency == order['buy_currency'],
                                         Order.sell_amount / Order.buy_amount >= order['buy_amount'] / order[
                                             'sell_amount']).first()
    if result == None:
        g.session.add(order_obj)
        g.session.commit()
        return

    order_obj.filled = datetime.now()
    order_obj.counterparty_id = result.id

    g.session.add(order_obj)
    g.session.commit()

    result.filled = datetime.now()
    result.counterparty_id = order_obj.id
    g.session.commit()

    # print(result.id, result.counterparty_id, order_obj.id, order_obj.counterparty_id)

    if order_obj.buy_amount > result.sell_amount:
        new_buy_amount = order_obj.buy_amount - result.sell_amount
        new_sell_amount = new_buy_amount * order_obj.sell_amount / order_obj.buy_amount

        new_order = {'buy_currency': order_obj.buy_currency, 'sell_currency': order_obj.sell_currency,
                     'buy_amount': new_buy_amount, 'sell_amount': new_sell_amount, 'sender_pk': order_obj.sender_pk,
                     'receiver_pk': order_obj.receiver_pk, 'creator_id': order_obj.id}
        # print(new_buy_amount, new_sell_amount)
        fill_order(new_order)

    if order_obj.buy_amount < result.sell_amount:
        new_sell_amount = result.sell_amount - order_obj.buy_amount
        new_buy_amount = new_sell_amount * result.buy_amount / result.sell_amount

        new_order = {'buy_currency': result.buy_currency, 'sell_currency': result.sell_currency,
                     'buy_amount': new_buy_amount, 'sell_amount': new_sell_amount, 'sender_pk': result.sender_pk,
                     'receiver_pk': result.receiver_pk, 'creator_id': result.id}
        # print(new_buy_amount, new_sell_amount)
        fill_order(new_order)


# def add_to_order(sender_pk, receiver_pk, buy_currency, sell_currency, buy_amount, sell_amount, signature):
#     order_obj = Order(sender_pk=sender_pk, receiver_pk=receiver_pk,
#                       buy_currency=buy_currency, sell_currency=sell_currency,
#                       buy_amount=buy_amount, sell_amount=sell_amount, signature=signature)
#     # print(order_obj.sender_pk, "here")
#     g.session.add(order_obj)
#     g.session.commit()


def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    m = json.dumps(d)
    log_obj = Log(logtime=datetime.now(), message=m)
    g.session.add(log_obj)
    g.session.commit()


""" End of helper methods """


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print(f"content = {json.dumps(content)}")
        columns = ["sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform"]
        fields = ["sig", "payload"]

        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        # Your code here
        # Note that you can access the database session using g.session

        # TODO: Check the signature
        payload = content.get('payload')
        platform = payload.get('platform')
        sig = content.get('sig')
        pk = payload.get('sender_pk')
        result = False

        if platform == 'Ethereum':

            msg = json.dumps(payload)
            encoded_msg = eth_account.messages.encode_defunct(text=msg)

            if eth_account.Account.recover_message(encoded_msg, signature=sig) == pk:
                result = True

        if platform == 'Algorand':
            msg = json.dumps(payload)

            if algosdk.util.verify_bytes(msg.encode('utf-8'), sig, pk):
                result = True

        print(result)

        # TODO: Add the order to the database
        if not result:
            log_message(payload)
            return jsonify(False)

        if result:
        # TODO: Fill the order
            order = {}
            order['buy_currency'] = payload.get('buy_currency')
            order['sell_currency'] = payload.get('sell_currency')
            order['buy_amount'] = payload.get('buy_amount')
            order['sell_amount'] = payload.get('sell_amount')
            order['sender_pk'] = payload.get('sender_pk')
            order['receiver_pk'] = payload.get('receiver_pk')
            fill_order(order)

        return jsonify(True)
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful


@app.route('/order_book')
def order_book():
    # Your code here
    # Note that you can access the database session using g.session
    datalist = []
    for row in g.session.query(Order).all():
        temp = {'sender_pk': row.sender_pk, 'receiver_pk': row.receiver_pk, 'buy_currency': row.buy_currency,
                'sell_currency': row.sell_currency, 'buy_amount': row.buy_amount, 'sell_amount': row.sell_amount,
                'signature': row.signature}
        # print(temp)
        datalist.append(temp)

    result = {'data': datalist}
    # print(result)
    return jsonify(result)


if __name__ == '__main__':
    app.run(port='5002')
