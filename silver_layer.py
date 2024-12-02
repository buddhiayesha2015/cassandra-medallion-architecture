from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = 'medallion_architecture'


def connect_cassandra():
    cluster = Cluster(contact_points=CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return session, cluster


def create_silver_table(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS silver_sales (
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
    print("Silver table 'silver_sales' created.")


def transform_and_load_silver(session):
    select_query = "SELECT * FROM bronze_sales;"
    rows = session.execute(select_query)

    insert_query = """
    INSERT INTO silver_sales (
        order_id, region, country, item_type, sales_channel, order_priority,
        order_date, ship_date, units_sold, unit_price, unit_cost,
        total_revenue, total_cost, total_profit
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    );
    """
    prepared = session.prepare(insert_query)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_size = 100
    count = 0

    for row in rows:
        cleaned_region = row.region.strip().title() if row.region else None
        cleaned_country = row.country.strip().title() if row.country else None
        cleaned_item_type = row.item_type.strip().title() if row.item_type else None
        cleaned_sales_channel = row.sales_channel.strip().title() if row.sales_channel else None
        cleaned_order_priority = row.order_priority.strip().upper() if row.order_priority else None

        units_sold = row.units_sold if row.units_sold >= 0 else 0

        batch.add(prepared, (
            row.order_id,
            cleaned_region,
            cleaned_country,
            cleaned_item_type,
            cleaned_sales_channel,
            cleaned_order_priority,
            row.order_date,
            row.ship_date,
            units_sold,
            row.unit_price,
            row.unit_cost,
            row.total_revenue,
            row.total_cost,
            row.total_profit
        ))

        count += 1
        if count % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if len(batch) > 0:
        session.execute(batch)
    print("Data transformed and loaded into Silver table 'silver_sales'.")


def main():
    session, cluster = connect_cassandra()
    create_silver_table(session)
    transform_and_load_silver(session)
    cluster.shutdown()
    print("Silver layer processing completed and connection closed.")


if __name__ == "__main__":
    main()
