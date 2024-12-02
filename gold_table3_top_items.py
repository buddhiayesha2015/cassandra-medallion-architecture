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


def create_gold_table3(session):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS gold_top_items (
        item_type text PRIMARY KEY,
        total_profit decimal
    );
    """
    session.execute(create_table_query)
    print("Gold table 'gold_top_items' created.")


def populate_gold_table3(session):
    select_query = "SELECT item_type, total_profit FROM silver_sales;"
    rows = session.execute(select_query)

    # Aggregation using a dictionary
    aggregation = {}
    for row in rows:
        if row.item_type:
            key = row.item_type
            if key not in aggregation:
                aggregation[key] = 0.0
            aggregation[key] += float(row.total_profit)

    # Sort items by total_profit and get top 10
    sorted_items = sorted(aggregation.items(), key=lambda x: x[1], reverse=True)[:10]

    # Insert top 10 items into Gold table
    insert_query = """
    INSERT INTO gold_top_items (
        item_type, total_profit
    ) VALUES (?, ?);
    """
    prepared = session.prepare(insert_query)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch_size = 100
    count = 0

    for item_type, total_profit in sorted_items:
        batch.add(prepared, (
            item_type,
            total_profit
        ))
        count += 1
        if count % batch_size == 0:
            session.execute(batch)
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    if len(batch) > 0:
        session.execute(batch)
    print("Gold table 'gold_top_items' populated with top 10 items by total profit.")


def main():
    session, cluster = connect_cassandra()
    create_gold_table3(session)
    populate_gold_table3(session)

    select_query = "SELECT * FROM gold_top_items;"
    rows = session.execute(select_query)
    print("\nData from 'gold_top_items':\n")
    for row in rows:
        print(row)

    cluster.shutdown()
    print("\nGold Table 3 processing completed and connection closed.")


if __name__ == "__main__":
    main()
