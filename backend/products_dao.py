from sql_connection import get_sql_connection
from datetime import datetime


class Database:
    def __init__(self,connection) -> None:
        self.cnx = connection

    def get_all_products(self):
        cursor = self.cnx.cursor()
        query = ("SELECT products.product_id, products.name, products.uom_id, products.price_per_unit, uom.uom_name "
                "FROM products INNER JOIN uom ON products.uom_id = uom.uom_id")

        cursor.execute(query)
        response = []
        for (product_id, name, uom_id, price_per_unit,uom_name) in cursor:
            response.append(
                {
                    'product_id':product_id,
                    'name':name,
                    "uom_id":uom_id,
                    "price_per_unit":price_per_unit,
                    "uom_name":uom_name
                }
            )
        return response

    def insert_new_product(self,product):
        cursor = self.cnx.cursor()
        query = ("INSERT INTO products "
        "(name,uom_id,price_per_unit)"
        "VALUES (%s,%s,%s)")
        data = (product['product_name'],product['uom_id'],product['price_per_unit'])
        cursor.execute(query,data)
        self.cnx.commit()
        return cursor.lastrowid
    
    def delete_product(self,product_id):
        cursor = self.cnx.cursor()
        query = ("DELETE from products WHERE product_id="+str(product_id))
        cursor.execute(query)
        self.cnx.commit()
        return cursor.lastrowid

    def get_uoms(self):
        cursor = self.cnx.cursor()
        query = ("SELECT * FROM uom")
        cursor.execute(query)
        response = []
        for (uom_id,uom_name) in cursor:
            response.append(
                {
                    "uom_id":uom_id,
                    "uom_name":uom_name
                }
            )
        return response

    def insert_order(self, order):
        cursor = self.cnx.cursor()

        order_query = ("INSERT INTO orders "
                "(customer_name, total, datetime)"
                "VALUES (%s, %s, %s)")
        order_data = (order['customer_name'], order['grand_total'], datetime.now())

        cursor.execute(order_query, order_data)
        order_id = cursor.lastrowid

        order_details_query = ("INSERT INTO order_details "
                            "(order_id, product_id, quantity, total_price)"
                            "VALUES (%s, %s, %s, %s)")

        order_details_data = []
        for order_detail_record in order['order_details']:
            order_details_data.append([
                order_id,
                int(order_detail_record['product_id']),
                float(order_detail_record['quantity']),
                float(order_detail_record['total_price'])
            ])
        cursor.executemany(order_details_query, order_details_data)

        self.cnx.commit()

        return order_id
    
    def get_order_details(self, order_id):
        cursor = self.cnx.cursor()

        query = "SELECT * from order_details where order_id = %s"

        query = "SELECT order_details.order_id, order_details.quantity, order_details.total_price, "\
                "products.name, products.price_per_unit FROM order_details LEFT JOIN products on " \
                "order_details.product_id = products.product_id where order_details.order_id = %s"

        data = (order_id, )

        cursor.execute(query, data)

        records = []
        for (order_id, quantity, total_price, product_name, price_per_unit) in cursor:
            records.append({
                'order_id': order_id,
                'quantity': quantity,
                'total_price': total_price,
                'product_name': product_name,
                'price_per_unit': price_per_unit
            })

        # cursor.close()

        return records

    def get_all_orders(self):
        cursor = self.cnx.cursor()
        query = ("SELECT * FROM orders")
        cursor.execute(query)
        response = []
        for (order_id, customer_name, total, dt) in cursor:
            response.append({
                'order_id': order_id,
                'customer_name': customer_name,
                'total': total,
                'datetime': dt,
            })

        cursor.close()

        # append order details in each order
        for record in response:
            record['order_details'] = self.get_order_details(record['order_id'])

        return response
        

if __name__ == "__main__":
    jr = Database(get_sql_connection())
    print(jr.delete_product(32))