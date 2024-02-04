from flask import Flask,request,render_template,jsonify
import products_dao
from sql_connection import get_sql_connection
import json



app = Flask(__name__)

connection = get_sql_connection()

database = products_dao.Database(connection)

@app.route("/getProducts",methods=['GET'])
def get_products():
    products = database.get_all_products()
    response = jsonify(products)
    response.headers.add('Access-Control-Allow-Origin','*')
    return response

@app.route('/deleteProduct', methods=['POST'])
def delete_product():
    return_id = database.delete_product(request.form['product_id'])
    print(request.form['product_id'])
    response = jsonify({
        'product_id': return_id
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


@app.route('/getUOM', methods=['GET'])
def get_uom():
    response = database.get_uoms()
    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/insertProduct', methods=['POST'])
def insert_product():
    request_payload = json.loads(request.form['data'])
    product_id = database.insert_new_product(request_payload)
    response = jsonify({
        'product_id': product_id
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/insertOrder', methods=['POST'])
def insert_order():
    request_payload = json.loads(request.form['data'])
    order_id = database.insert_order(request_payload)
    response = jsonify({
        'order_id': order_id
    })
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/getAllOrders', methods=['GET'])
def get_all_orders():
    response = database.get_all_orders()
    response = jsonify(response)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


if __name__ == "__main__":
    print("Server Starting")
    app.run(port=5000)