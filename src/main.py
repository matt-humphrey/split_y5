import marimo

__generated_with = "0.14.10"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    import odyssey.core as od
    return od, pl


@app.cell
def _():
    from config import RAW_DATA, PROCESSED_DATA, DATASETS
    config = DATASETS["G205"]
    return PROCESSED_DATA, RAW_DATA, config


@app.cell
def _(RAW_DATA, od):
    lf, meta = od.read_sav(RAW_DATA, "G205_PAdata.sav")
    df = lf.collect()
    meta = od.reformat_metadata(meta)
    return df, meta


@app.cell
def _(config, df, pl):
    variables = pl.col(
        "G205_PBP1", "G205_PBP2", "G205_PBP3", "G205_PBP4", 
        "G205_PBP5", "G205_P10", "G205_P11", "G205_BPCD"
    )

    gen1_columns_sorted = [
        'ID', 'G105_XDAT', 'G105_A1', 'G105_A2', 'G105_BP1', 
        'G105_BP2', 'G105_BP3', 'G105_BP4', 'G105_BP5', 'G105_BPCD'
    ]

    gen2 = df.drop(variables)

    gen1 = (
        df
        .select(pl.col("ID", "G205_XDAT"), variables)
        .with_columns(G205_BPCD=pl.col("G205_BPCD").replace({"-99": None})) # Change to null to drop row with no data
        .rename(config.get("rename"))
        .drop_nulls(subset=pl.exclude("ID", "G105_XDAT"))
        .select(gen1_columns_sorted)
    )
    return gen1, gen2


@app.cell
def _(config, meta):
    # Rename metadata
    rename_map = config.get("rename")

    gen1_meta = {
        param: {rename_map.get(k, k): v for k, v in inner_dict.items() if k in rename_map or k == "ID"}
        for param, inner_dict in meta.items()
    }
    return (gen1_meta,)


@app.cell
def _(RAW_DATA, gen1, od, pl):
    # Determine Gen1 IDs
    relations, _ = od.read_sav(RAW_DATA, "Relationships_file_July 2018.sav")
    relations = relations.select("Gen2_ID", "Gen1_B_MID", "Gen1_B_FID").collect()

    harmonised_gen1 = (
        gen1
        .join(relations, left_on="ID", right_on="Gen2_ID")
        .with_columns(
            pl.when(pl.col("G105_BPCD").is_in(("A", "B", "C")))
            .then(pl.col("Gen1_B_MID"))
            .when(pl.col("G105_BPCD").is_in(("D", "E")))
            .then(pl.col("Gen1_B_FID"))
            .otherwise(None)
            .alias("ID")
        )
        .drop_nulls() # drop two instances where there is a missing value for BPCD - Gen1 parent can't be identified
        .drop("Gen1_B_MID", "Gen1_B_FID")
        .unique(subset="ID") # Filter out parent values duplicated for siblings
        .sort(by="ID")
    )
    return harmonised_gen1, relations


@app.cell
def _(PROCESSED_DATA, gen1_meta, gen2, harmonised_gen1, meta, od):
    od.write_sav(PROCESSED_DATA/"G105_PA.sav", harmonised_gen1.lazy(), gen1_meta)
    od.write_sav(PROCESSED_DATA/"G205_PA.sav", gen2.lazy(), meta)
    return


@app.cell
def _(mo, relations):
    mo.ui.table(relations)
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
