import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

# Recupera os argumentos do job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Inicializa o contexto do Spark e Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Regras padrão de qualidade de dados
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""


# Função auxiliar para criar DynamicFrames a partir do S3
def read_csv_from_s3(path, transformation_ctx):
    return glueContext.create_dynamic_frame.from_options(
        format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path], "recurse": True},
        transformation_ctx=transformation_ctx,
    )


# Carregamento dos dados
artist_df = read_csv_from_s3(
    "s3://project-spt-biss/staging/spotify_artist_data_2023.csv", "artist_node"
)
album_df = read_csv_from_s3(
    "s3://project-spt-biss/staging/spotify-albums_data_2023.csv", "album_node"
)
tracks_df = read_csv_from_s3(
    "s3://project-spt-biss/staging/spotify_tracks_data_2023.csv", "tracks_node"
)

# Junção de artistas com álbuns
join_album_artist_df = Join.apply(
    frame1=artist_df,
    frame2=album_df,
    keys1=["id"],
    keys2=["artist_id"],
    transformation_ctx="join_album_artist",
)

# Junção com as faixas
join_with_tracks_df = Join.apply(
    frame1=join_album_artist_df,
    frame2=tracks_df,
    keys1=["track_id"],
    keys2=["id"],
    transformation_ctx="join_with_tracks",
)

# Remoção de campos desnecessários (atualmente vazio, pode ser atualizado futuramente)
drop_fields_df = DropFields.apply(
    frame=join_with_tracks_df, paths=[], transformation_ctx="drop_fields"
)

# Avaliação da qualidade dos dados
EvaluateDataQuality().process_rows(
    frame=drop_fields_df,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)

# Gravação no destino S3
output_path = "s3://project-spt-biss/datawarehouse/"
glueContext.write_dynamic_frame.from_options(
    frame=drop_fields_df,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": output_path, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="destination",
)

# Finaliza o job
job.commit()