import itertools
from pprint import pprint

from pyspark.sql import DataFrame, Row, SparkSession


def join_products_with_categories(
    prods: DataFrame, cats: DataFrame, prods_to_cats: DataFrame
) -> tuple[list[tuple[str, str]], list[str]]:
    df = (
        prods.join(prods_to_cats, on="product_id", how="left")
        .join(cats, on="category_id", how="left")
        .select(["product_name", "category_name"])
    )

    products_with_categories, uncategorized_products = (
        df.where(df["category_name"].isNotNull()).rdd.map(tuple).collect(),
        df.where(df["category_name"].isNull())
        .select("product_name")
        .toPandas()["product_name"]
        .tolist(),
    )

    return products_with_categories, uncategorized_products


def main() -> None:
    spark = SparkSession.builder.getOrCreate()  # type: ignore

    product_ids = itertools.count(1)
    products = spark.createDataFrame(
        [
            Row(product_id=next(product_ids), product_name="Apple"),
            Row(product_id=next(product_ids), product_name="Peach"),
            Row(product_id=next(product_ids), product_name="Milk"),
            Row(product_id=next(product_ids), product_name="Cheese"),
            Row(product_id=next(product_ids), product_name="Butter"),
            Row(product_id=next(product_ids), product_name="Candy bar"),
            Row(product_id=next(product_ids), product_name="Chewing gum"),
        ]
    )

    category_ids = itertools.count(1)
    categories = spark.createDataFrame(
        [
            Row(category_id=next(category_ids), category_name="Produce"),
            Row(category_id=next(category_ids), category_name="Dairy"),
            Row(category_id=next(category_ids), category_name="Discount"),
        ]
    )

    product_categories = spark.createDataFrame(
        [
            Row(product_id=1, category_id=1),
            Row(product_id=2, category_id=1),
            Row(product_id=2, category_id=3),
            Row(product_id=3, category_id=2),
            Row(product_id=4, category_id=2),
            Row(product_id=5, category_id=2),
            Row(product_id=5, category_id=3),
            Row(product_id=7, category_id=3),
        ]
    )

    (
        products_with_categories,
        uncategorized_products,
    ) = join_products_with_categories(
        products,
        categories,
        product_categories,
    )

    pprint(products_with_categories)
    pprint(uncategorized_products)


if __name__ == "__main__":
    main()
