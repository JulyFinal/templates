import time
import multiprocessing

import polars as pl


from pip._vendor.rich.progress import track


def custom_function(x):
    return x + 1


def parallel_apply(function, column):
    with multiprocessing.get_context("spawn").Pool() as pool:
        return pl.Series(pool.imap(function, track(column)))


if __name__ == "__main__":
    df = pl.DataFrame(
        {
            "a": [i for i in range(30000)],
            "b": [i for i in range(30000)],
        }
    )
    t1 = time.time()
    df = df.with_columns(
        add_col=pl.col("a").map_batches(lambda col: map(custom_function, col))
    )
    t2 = time.time()

    # print(df)

    df = df.with_columns(
        add_col=pl.col("a").map_batches(
            lambda col: parallel_apply(custom_function, col)
        )
    )
    t3 = time.time()

    print(f"not use parallel cost time: {t2-t1}")
    print(f"use parallel cost time: {t3-t2}")
