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


def create_gold_table1(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS gold_total_sales_region_country (
        region text,
        country text,
        total_revenue decimal,
        total_cost decimal,
        total_profit decimal,
        PRIMARY KEY ((region), country)
    );
    """
    session.execute(create_table_query)
    print("Gold table 'gold_total_sales_region_country' created.")


def populate_gold_table1(session):
    select_query = "SELECT region, country, total_revenue, total_cost, total_profit FROM silver_sales;"
    rows = session.execute(select_query)

    aggregation = {}
    for row in rows:
        key = (row.region, row.country)
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
    INSERT INTO gold_total_sales_region_country (
        region, country, total_revenue, total_cost, total_profit
    ) VALUES (?, ?, ?, ?, ?);
    """
    prepared = session.prepare(insert_query)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_size = 100
    count = 0

    for (region, country), totals in aggregation.items():
        batch.add(prepared, (
            region,
            country,
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
    print("Gold table 'gold_total_sales_region_country' populated with aggregated data.")


def main():
    session, cluster = connect_cassandra()
    create_gold_table1(session)
    populate_gold_table1(session)

    # Fetch data from gold_total_sales_region_country
    select_query = "SELECT * FROM gold_total_sales_region_country;"
    rows = session.execute(select_query)
    print("\nData from 'gold_total_sales_region_country':\n")
    for row in rows:
        print(row)

    cluster.shutdown()
    print("\nGold Table 1 processing completed and connection closed.")


if __name__ == "__main__":
    main()
