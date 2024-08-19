import pyodbc
import json
from decimal import Decimal
from pymongo import MongoClient
import base64
import uuid
from datetime import datetime


class DataHandler:
    @staticmethod
    def convert_to_serializable(obj):
        if isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, bytes):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return base64.b64encode(obj).decode('ascii')
        return obj

    @staticmethod
    def create_table_json(customer_id, memo, dry_clean_memo, laundry_memo, first_name, last_name, phone1_area,
                          phone1_no1, phone1_no2, customer_no, email):
        def format_phone_number(area, no1, no2):
            return f"{area}{no1}{no2}"

        return {
            "firstName": first_name.upper(),
            "lastName": last_name.upper(),
            "phone": format_phone_number(phone1_area, phone1_no1, phone1_no2),
            "email": email,
            "import": True,
            "ldNotes": laundry_memo,
            "dcNotes": dry_clean_memo,
            "notes": memo,
            "customerID": "A" + str(customer_id),
            "customerNo": customer_no
        }

    @staticmethod
    def create_drop_item_json(name, price):
        return {
            "itemName": name.upper(),
            "price": round(float(price), 2),
            "sku": str(uuid.uuid4()),
            "upsert": True
        }

    @staticmethod
    def create_generated_credit_json(customer_id, reason, create_date, amount, applied):
        if isinstance(create_date, datetime):
            create_date_str = create_date.strftime('%Y-%m-%d')
        else:
            create_date_str = datetime.strptime(create_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        credit_amount = round(float(amount) - float(applied), 2)
        if credit_amount > 0:
            return {
                "customerID": 'A' + str(customer_id),
                "date": create_date_str,
                "dateTime": create_date_str,
                "creditAmount": credit_amount,
                "paymentID": str(uuid.uuid4()),
                "import": True,
                "reason": reason
            }
        else:
            return None

    @staticmethod
    def create_drop_item_category_json(drop_item_category_id, name, pieces, price, order_number):
        category_mapping = {
            6: 'D',
            7: 'L',
            8: 'H',
            9: 'S',
            10: 'W'
        }
        drop_item_category_id = int(drop_item_category_id)
        category = category_mapping.get(drop_item_category_id, '')
        return {
            "category": category,
            "itemName": name.upper(),
            "pieces": int(pieces),
            "price": round(float(price), 2),
            "sort": int(order_number),
            "sku": str(uuid.uuid4())
        }

    @staticmethod
    def create_real_drop_json(customer_id, create_date, sub_total, up_charge, env_charge, pieces, tax, total, balance,
                              due_date, rack, real_drop_id, pickup_date):
        def format_date(date):
            if isinstance(date, datetime):
                return date.strftime('%Y-%m-%d')
            else:
                return datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')

        date = format_date(create_date)
        due_date = format_date(due_date) if due_date else None
        pickup_date = format_date(pickup_date) if pickup_date else None

        order_status = 'inProcess'
        if rack:
            order_status = 'complete'
            if pickup_date:
                order_status = 'pickedUp'

        payment_status = 'complete' if float(balance) <= 0 else None

        return {
            "date": date,
            "dateTime": date,
            "orderStatus": order_status,
            "subTotal": round(float(sub_total), 2),
            "customerID": 'A' + str(customer_id),
            "envFee": round(float(env_charge), 2),
            "totalPieces": int(pieces),
            "salesTax": round(float(tax), 2),
            "total": round(float(total), 2),
            "invoice": '01-' + real_drop_id,
            "readyDate": due_date,
            "pickupDate": pickup_date,
            "rackLocation": rack,
            "paymentStatus": payment_status,
            "importBalance": float(balance),
            "import": True
        }

    @staticmethod
    def create_alteration_json(name, price):
        return {
            "itemName": name.upper(),
            "price": round(float(price), 2),
            "sku": str(uuid.uuid4()),
            "upsert": True
        }


class SQLServer:
    def __init__(self):
        self.server = 'DESKTOP-ME9PE2T'
        self.database = 'CleanerPos'
        self.cnxn_str = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes"

    def get_cursor(self):
        cnxn = pyodbc.connect(self.cnxn_str)
        return cnxn.cursor()

    def execute_query(self, table_name):
        if table_name == 'TCustomer':
            query = f"SELECT CustomerID, Memo, DryCleanMemo, LaundryMemo, FirstName, LastName, Phone1Area, Phone1No1, Phone1No2, CustomerNo, Email FROM {table_name} WHERE IsActive = 1"
        elif table_name == 'TDropItemUpcharge':
            query = "SELECT Name, Price FROM TDropItemUpcharge ORDER BY Price ASC"
        elif table_name == 'TGeneratedCredit':
            query = "SELECT CustomerID, CreateDate, Amount, Applied, Reason FROM TGeneratedCredit"
        elif table_name == 'TDropItem':
            query = "SELECT DropItemCategoryID, Name, Pieces, Price, OrderNumber FROM TDropItem ORDER BY Price ASC"
        elif table_name == 'TRealDrop':
            query = "SELECT CustomerID, CreateDate, SubTotal, UpCharge, EnvCharge, Pieces, Tax, Total, Balance, DueDate, Rack, RealDropID, PickupDate FROM TRealDrop WHERE IsVoid = 1"
        elif table_name == 'TAlteration':
            query = "SELECT Name, Price FROM TAlteration ORDER BY Price ASC"
        else:
            raise ValueError(f"Unknown table name: {table_name}")
        cursor = self.get_cursor()
        cursor.execute(query)
        return cursor


class MongoDB:
    def __init__(self, db_name, collection_name):
        self.uri = "##"
        self.db_name = db_name
        self.collection_name = collection_name
        self.client = MongoClient(self.uri)
        self.db = self.client[self.db_name]
        self.collection = self.db[self.collection_name]

    def import_json_to_table(self, json_filepath):
        with open(json_filepath, 'r') as file:
            data = json.load(file)
        if isinstance(data, list):
            self.collection.insert_many(data)
        else:
            self.collection.insert_one(data)
        print(f"Data successfully imported into {self.db_name}.{self.collection_name}")
