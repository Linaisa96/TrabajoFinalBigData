from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver layer transformation: Clean and validate descuentos POS data
@dp.table(
    name="workspace.silver.dinamico_de_descuentos_pos",
    comment="Silver layer: cleaned and validated descuentos POS data with standardized column names and data quality checks",
    cluster_by_auto=True
)
@dp.expect_all_or_drop({
    "valid_fecha_redencion": "fecha_redencion IS NOT NULL",
    "valid_unidades_redimidas": "unidades_redimidas >= 0",
    "valid_valor_venta": "valor_venta_sin_impuesto >= 0",
    "valid_evento_pos_cd": "evento_pos_cd IS NOT NULL"
})
@dp.expect_all({
    "valid_fecha_inicial_emision": "fecha_inicial_emision <= fecha_final_emision OR fecha_inicial_emision IS NULL OR fecha_final_emision IS NULL",
    "valid_fecha_inicial_redencion": "fecha_inicial_redencion <= fecha_final_redencion OR fecha_inicial_redencion IS NULL OR fecha_final_redencion IS NULL",
    "recent_data": "fecha_redencion >= '2020-01-01'"
})
def dinamico_de_descuentos_pos():
    """
    Bronze to Silver transformation for descuentos POS data.
    
    Transformations applied:
    - Column names standardized to snake_case
    - Numeric columns coalesced with default values
    - String columns trimmed
    - Audit columns added (processing_timestamp, source_table)
    - Data quality expectations enforced
    """
    # Read from bronze layer using streaming read for incremental processing
    df = spark.readStream.table("workspace.bronze.dinamico_de_descuentos_pos")
    
    # Column name mapping: original -> cleaned snake_case names
    column_mapping = {
        "Dependencia DependenciaCD": "dependencia_cd",
        "Dependencia DESC": "dependencia_desc",
        "ZONA2": "zona_cd",
        "Zona3": "zona_desc",
        "SUBZONA4": "subzona_cd",
        "Subzona5": "subzona_desc",
        "DEPARTAMENTO6": "departamento_cd",
        "Departamento7": "departamento_desc",
        "CIUDAD8": "ciudad_cd",
        "Ciudad9": "ciudad_desc",
        "Clima": "clima",
        "Evento Fidelizacion EventoPosCD": "evento_pos_cd",
        "Evento Fidelizacion FechaInicialEmision": "fecha_inicial_emision",
        "Evento Fidelizacion Descripcion Evento": "descripcion_evento",
        "Evento Fidelizacion Fecha Final Emision": "fecha_final_emision",
        "Evento Fidelizacion Texto Mensaje": "texto_mensaje",
        "Evento Fidelizacion FechaI inicial Redencion": "fecha_inicial_redencion",
        "Evento Fidelizacion FechaFinalRedencionEvID": "fecha_final_redencion",
        "Evento Fidelizacion ProcedenciaEvento": "procedencia_evento",
        "Direccion ID": "direccion_id",
        "Direccion DESC": "direccion_desc",
        "Sublinea ID": "sublinea_id",
        "Sublinea DESC": "sublinea_desc",
        "Categoria ID sublinea": "categoria_id_sublinea",
        "Categoria ID categoria": "categoria_id",
        "Categoria DESC": "categoria_desc",
        "Subcategoria IDsubcategoria": "subcategoria_id",
        "Subcategoria DESC": "subcategoria_desc",
        "Plu PluCD": "plu_cd",
        "Plu DESC": "plu_desc",
        "Marca ID": "marca_id",
        "Marca DESC": "marca_desc",
        "Proveedor Plu-Dep Nit": "proveedor_nit",
        "Proveedor Plu-Dep Nombre Proveedor": "proveedor_nombre",
        "Fecha Redencion FechaRedencion": "fecha_redencion",
        "# Unidades Redimidas": "unidades_redimidas",
        "$ Valor Venta Sin Impuesto Despues de Descuento": "valor_venta_sin_impuesto",
        "$ Valor Evento Redimido Sin Impuesto": "valor_evento_redimido",
        "$ Costo": "costo",
        "$ Costo Neto - Plu Dep": "costo_neto"
    }
    
    # Rename columns to snake_case
    for old_name, new_name in column_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    # Add audit columns
    df = df.withColumn("processing_timestamp", F.current_timestamp())
    df = df.withColumn("source_table", F.lit("workspace.bronze.dinamico_de_descuentos_pos"))
    
    # Cast numeric columns to appropriate types and handle nulls
    df = df.withColumn("unidades_redimidas", F.coalesce(F.col("unidades_redimidas"), F.lit(0)))
    df = df.withColumn("valor_venta_sin_impuesto", F.coalesce(F.col("valor_venta_sin_impuesto"), F.lit(0.0)))
    df = df.withColumn("valor_evento_redimido", F.coalesce(F.col("valor_evento_redimido"), F.lit(0.0)))
    df = df.withColumn("costo", F.coalesce(F.col("costo"), F.lit(0.0)))
    df = df.withColumn("costo_neto", F.coalesce(F.col("costo_neto"), F.lit(0.0)))
    
    # Trim string columns to remove leading/trailing spaces
    string_columns = [
        "dependencia_desc", "zona_desc", "subzona_desc", "departamento_desc",
        "ciudad_desc", "clima", "descripcion_evento", "texto_mensaje",
        "direccion_desc", "sublinea_desc", "categoria_desc", "subcategoria_desc",
        "plu_desc", "marca_desc", "proveedor_nombre"
    ]
    
    for col_name in string_columns:
        df = df.withColumn(col_name, F.trim(F.col(col_name)))
    
    return df
