from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
import pandas as pd

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = 'medallion_architecture'


def create_keyspace(session):
    create_ks_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': '1'
    }};
    """
    session.execute(create_ks_query)
    print(f"Keyspace '{KEYSPACE}' is ready.")


def create_bronze_table(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS bronze_sales (
        order_id bigint PRIMARY KEY,
        region text,
        country text,
        item_type text,
        sales_channel text,
        order_priority text,
        order_date timestamp,
        ship_date timestamp,
        units_sold int,
        unit_price decimal,
        unit_cost decimal,
        total_revenue decimal,
        total_cost decimal,
        total_profit decimal
    );
    """
    session.execute(create_table_query)
    print("Bronze table 'bronze_sales' created.")


def import_bronze_data(session, csv_file_path):
    df = pd.read_csv(csv_file_path)
    df['Order Date'] = pd.to_datetime(df['Order Date'], format='%m/%d/%Y')
    df['Ship Date'] = pd.to_datetime(df['Ship Date'], format='%m/%d/%Y')
    insert_query = """
    INSERT INTO bronze_sales (
        order_id, region, country, item_type, sales_channel, order_priority,
        order_date, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    );
    """
    prepared = session.prepare(insert_query)
    batch_size = 100
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for index, row in df.iterrows():
        batch.add(prepared, (
            int(row['Order ID']),
            row['Region'].strip(),
            row['Country'].strip(),
            row['Item Type'].strip(),
            row['Sales Channel'].strip(),
            row['Order Priority'].strip(),
            row['Order Date'],
            row['Ship Date'],
            int(row['UnitsSold']),
            float(row['UnitPrice']),
            float(row['UnitCost']),
            float(row['TotalRevenue']),
            float(row['TotalCost']),
            float(row['TotalProfit'])
        ))
        if (index + 1) % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            print(f"Inserted {index + 1} records into 'bronze_sales'.")
    if len(batch) > 0:
        session.execute(batch)
        print(f"Inserted remaining records into 'bronze_sales'.")
    print("Data imported into Bronze table 'bronze_sales'.")


def main():
    cluster = Cluster(contact_points=CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect()
    create_keyspace(session)
    session.set_keyspace(KEYSPACE)
    create_bronze_table(session)
    import_bronze_data(session, 'sales_100.csv')
    cluster.shutdown()
    print("Bronze layer processing completed and connection closed.")


if __name__ == "__main__":
    main()
