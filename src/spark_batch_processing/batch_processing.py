from __future__ import print_function
import sys
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from graphframes import GraphFrame
from pyspark.sql import SparkSession


if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: batch_process <s3 file_path> <checkpoint_folder_path> <max limit group size> <save file_path>", file=sys.stderr)
        exit(-1)

    spark = SparkSession \
        .builder \
        .appName("connected_components") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    transaction_filename = sys.argv[1]
    checkpoint_folder_dir = sys.argv[2]
    group_size_limit = sys.argv[3]
    output_filename = sys.argv[4]

    df = spark.read.csv(transaction_filename, header=True, inferSchema=True)
    df = df.orderBy("id")

    # clean the table
    def _clean_table(df, fields):
        node = df
        for field in fields:
            node = node.drop(field)
        return node

    fields = ['cell', 'ipv4', 'en0', 'credit_card']

    ## df.withColumn('cell')
    # generate the
    nodes = _clean_table(df, fields)

    # rename the dataframe as new names with mark_number (eg: "mark_1")
    # filter the fields that only has the unique values

    def _unique_filter(df, fields):
        '''
        filter the rows with ALL unique values
        :param df: dataFrame
        :param fields: list of strings of the table header
        :return: a filtered dataFrame
        '''
        # add aux columns for filter
        for id, field in enumerate(fields):
            label = "mark_" + str(id)
            df = df.withColumn(label, (F.count(field).over(Window.partitionBy(field)) == 1))

        # filter the table
        df = df.filter(~(True & df["mark_" + str(i)] for i in range(len(fields))))

        # drop the aux columns
        for id, field in enumerate(fields):
            label = "mark_" + str(id)
            df = df.drop(label)
        return df


    # build the graph
    def _build_graph(nodes, df):
        '''

        :param nodes: dataframe for the nodes
        :param edges: dataFrame for the edges
        :return: dataFrame for the graph
        '''
        # rename the
        edges = df.withColumnRenamed('id1', 'src').withColumnRenamed('id2', 'dst')
        g = GraphFrame(nodes, edges)
        return g

    # filter the dataset before building the graph
    df = _unique_filter(df, fields)
    g  = _build_graph(nodes, df)


    # rename the columns
    df1 = df.selectExpr("id as id1", "order as order1", "cell as cell1", "ipv4 as ipv41", "en0 as en01")
    df2 = df.selectExpr("id as id2", "order as order2", "cell as cell2", "ipv4 as ipv42", "en0 as en02")

    # cross join the table
    joint_df = df1.crossJoin(df2).filter(df1["id1"] < df2["id2"])
    df3 = joint_df.withColumn('cell1', joint_df.cell1 == joint_df.cell2). \
        withColumn('ipv41', joint_df.ipv41 == joint_df.ipv42). \
        withColumn('en01', joint_df.en01 == joint_df.en02)
    df3 = df3.drop('cell2').drop('ipv42').drop('en02').drop('order1').drop('order2')
    df3 = df3.drop('cell1').drop('ipv41').drop('en01')

    # build the edge relationship
    edges = df3.withColumn('relationship', lit(1))

    # build the edge information dataframe with header "src" and "dst"
    # build the graph
    g = _build_graph(nodes, edges)
    #    g.vertices.show()
    #    g.edges.show()
    # save checkpoint
    spark.sparkContext.setCheckpointDir(checkpoint_folder_dir)

    # find the connected components id for each component
    result = g.connectedComponents()

    # group the connected components with the same component ID
    grouped_df = result.select("id", "component").groupBy("component")
    sorted_df = grouped_df.count().orderBy("count", ascending=False)

    # any size that is greater than the limit will be listed
    filtered_df = sorted_df.filter(sorted_df["count"] > group_size_limit)

    # black list records the id in a list that has more than the max limit components
    detected_list = result.select("id").rdd.flatMap(lambda x: x).collect()

    df_final = df[df["id"].isin(detected_list)]


    spark.stop()
