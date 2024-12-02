from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

CASSANDRA_HOSTS = ['127.0.0.1']
CASSANDRA_PORT = 9042
KEYSPACE = 'medallion_architecture'


def connect_cassandra():
    cluster = Cluster(contact_points=CASSANDRA_HOSTS, port=CASSANDRA_PORT)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    return session, cluster


def create_gold_table2(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS gold_monthly_sales (
        year int,
        month int,
        total_revenue decimal,
        total_cost decimal,
        total_profit decimal,
        PRIMARY KEY ((year, month))
    );
    """
    session.execute(create_table_query)
    print("Gold table 'gold_monthly_sales' created.")


def populate_gold_table2(session):
    select_query = "SELECT order_date, total_revenue, total_cost, total_profit FROM silver_sales;"
    rows = session.execute(select_query)

    aggregation = {}
    for row in rows:
        order_date = row.order_date
        if order_date:
            year = order_date.year
            month = order_date.month
            key = (year, month)
            if key not in aggregation:
                aggregation[key] = {
                    'total_revenue': 0.0,
                    'total_cost': 0.0,
                    'total_profit': 0.0
                }
            aggregation[key]['total_revenue'] += float(row.total_revenue)
            aggregation[key]['total_cost'] += float(row.total_cost)
            aggregation[key]['total_profit'] += float(row.total_profit)

    insert_query = """
    INSERT INTO gold_monthly_sales (
        year, month, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?);
    """
    prepared = session.prepare(insert_query)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_size = 100
    count = 0

    for (year, month), totals in aggregation.items():
        batch.add(prepared, (
            year,
            month,
            totals['total_revenue'],
            totals['total_cost'],
            totals['total_profit']
        ))
        count += 1
        if count % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if len(batch) > 0:
        session.execute(batch)
    print("Gold table 'gold_monthly_sales' populated with aggregated data.")


def main():
    session, cluster = connect_cassandra()
    create_gold_table2(session)
    populate_gold_table2(session)

    select_query = "SELECT * FROM gold_monthly_sales;"
    rows = session.execute(select_query)
    print("\nData from 'gold_monthly_sales':\n")
    for row in rows:
        print(row)

    cluster.shutdown()
    print("\nGold Table 2 processing completed and connection closed.")


if __name__ == "__main__":
    main()
