from time import sleep
import psycopg2.errors
import pandas as pd
import psycopg2
import tqdm


def postgres_upsert(engine, df, table, conflict_column):
    if df.shape[0] > 0:
        print(f"\nUpserting {table} on conflict {conflict_column}")
        sleep(0.5)
        con = engine.raw_connection()
        cur = con.cursor()
        cols = ','.join(list(df))
        v = ','.join(['%s'] * len(list(df)))
        updates = ','.join([x + '=%s' for x in list(df)[1:]])
        rows = [list(x) for x in df.to_numpy()]
        qry = f"insert into {table} ({cols}) values ({v}) on conflict ({conflict_column}) do update set {updates}"
        for i in tqdm.tqdm(rows):
            i.extend(i[1:])
            j = [None if pd.isna(x) else x for x in i]
            try:
                cur.execute(qry, tuple(j))
            except psycopg2.errors.FeatureNotSupported:
                raise
            except psycopg2.ProgrammingError:
                raise
            con.commit()
        con.commit()
        con.close()
    else:
        print('\nEmpty dataframe on upsert!')
