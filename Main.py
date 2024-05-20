from db_config import SQLServer, MongoDB, DataHandler
import json

def process_table(table_name, data_handler_function, db_name, collection_name):
    # Setup SQL Server connection
    sql_server = SQLServer()

    # Execute SQL query
    cursor = sql_server.execute_query(table_name)

    # Prepare data for MongoDB import
    json_list = []
    while True:
        rows = cursor.fetchmany(100)  # Adjust batch size as needed
        if not rows:
            break
        for row in rows:
            if table_name == "TCustomer":
                json_data = data_handler_function(*[DataHandler.convert_to_serializable(col) for col in row])
            elif table_name == "TDropItemUpcharge":
                name, price = DataHandler.convert_to_serializable(row[0]), DataHandler.convert_to_serializable(row[1])
                json_data = data_handler_function(name, price)
            elif table_name == "TGeneratedCredit":
                customer_id = DataHandler.convert_to_serializable(row[0])
                create_date = DataHandler.convert_to_serializable(row[1])
                amount = DataHandler.convert_to_serializable(row[2])
                applied = DataHandler.convert_to_serializable(row[3])
                reason = DataHandler.convert_to_serializable(row[4])
                json_data = data_handler_function(customer_id, reason, create_date, amount, applied)
            elif table_name == "TDropItem":
                drop_item_category_id = DataHandler.convert_to_serializable(row[0])
                name = DataHandler.convert_to_serializable(row[1])
                pieces = DataHandler.convert_to_serializable(row[2])
                price = DataHandler.convert_to_serializable(row[3])
                order_number = DataHandler.convert_to_serializable(row[4])
                json_data = data_handler_function(drop_item_category_id, name, pieces, price, order_number)
            elif table_name == "TRealDrop":
                customer_id = DataHandler.convert_to_serializable(row[0])
                create_date = DataHandler.convert_to_serializable(row[1])
                sub_total = DataHandler.convert_to_serializable(row[2])
                up_charge = DataHandler.convert_to_serializable(row[3])
                env_charge = DataHandler.convert_to_serializable(row[4])
                pieces = DataHandler.convert_to_serializable(row[5])
                tax = DataHandler.convert_to_serializable(row[6])
                total = DataHandler.convert_to_serializable(row[7])
                balance = DataHandler.convert_to_serializable(row[8])
                due_date = DataHandler.convert_to_serializable(row[9])
                rack = DataHandler.convert_to_serializable(row[10])
                real_drop_id = DataHandler.convert_to_serializable(row[11])
                pickup_date = DataHandler.convert_to_serializable(row[12])
                json_data = data_handler_function(customer_id, create_date, sub_total, up_charge, env_charge, pieces, tax, total, balance, due_date, rack, real_drop_id, pickup_date)
            elif table_name == "TAlteration":
                name = DataHandler.convert_to_serializable(row[0])
                price = DataHandler.convert_to_serializable(row[1])
                json_data = data_handler_function(name, price)
            if json_data:
                json_list.append(json_data)

    # Write to JSON (if needed)
    json_filepath = table_name + '.json'
    with open(json_filepath, 'w') as json_file:
        json.dump(json_list, json_file, indent=4)

    # MongoDB import
    mongodb = MongoDB(db_name, collection_name)
    mongodb.import_json_to_table(json_filepath)
    cursor.close()

def process_all_tables(tables):
    for table_name, config in tables.items():
        print(f"Processing table: {table_name}")
        process_table(table_name, config["data_handler_function"], config["db_name"], config["collection_name"])

def display_menu(tables):
    print("Select the table to process:")
    for i, table_name in enumerate(tables.keys(), 1):
        print(f"{i}. {table_name}")
    print(f"{len(tables) + 1}. All tables")

def main():
    tables = {
        "TCustomer": {
            "data_handler_function": DataHandler.create_table_json,
            "db_name": "CleanMax",
            "collection_name": "TCustomer"
        },
        "TDropItemUpcharge": {
            "data_handler_function": DataHandler.create_drop_item_json,
            "db_name": "CleanMax",
            "collection_name": "TDropItemUpcharge"
        },
        "TGeneratedCredit": {
            "data_handler_function": DataHandler.create_generated_credit_json,
            "db_name": "CleanMax",
            "collection_name": "TGeneratedCredit"
        },
        "TDropItem": {
            "data_handler_function": DataHandler.create_drop_item_category_json,
            "db_name": "CleanMax",
            "collection_name": "TDropItem"
        },
        "TRealDrop": {
            "data_handler_function": DataHandler.create_real_drop_json,
            "db_name": "CleanMax",
            "collection_name": "TRealDrop"
        },
        "TAlteration": {
            "data_handler_function": DataHandler.create_alteration_json,
            "db_name": "CleanMax",
            "collection_name": "TAlteration"
        }
    }

    display_menu(tables)
    choice = int(input("Enter the number corresponding to your choice: "))

    if 1 <= choice <= len(tables):
        table_name = list(tables.keys())[choice - 1]
        config = tables[table_name]
        process_table(table_name, config["data_handler_function"], config["db_name"], config["collection_name"])
    elif choice == len(tables) + 1:
        process_all_tables(tables)
    else:
        print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
