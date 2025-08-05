import pandas as pd
import matplotlib.pyplot as plt
import base64
import io
import requests
from bs4 import BeautifulSoup
import duckdb


# Helper function to encode matplotlib plots as base64
def encode_plot_to_base64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    buf.seek(0)
    image_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plt.close(fig)
    return f"data:image/png;base64,{image_base64}"


# 1. Handle Wikipedia Scraping
def handle_scraping(questions: list[str]):
    url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"class": "wikitable"})

    if table is None:
        return {"error": "Could not find data table on Wikipedia page"}

    df = pd.read_html(str(table))[0]

    # Clean columns if multi-level
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(0)

    # Clean and convert 'Worldwide gross' column
    if "Worldwide gross" in df.columns:
        df["Worldwide gross"] = df["Worldwide gross"].astype(str).str.replace(r"[^\d.]", "", regex=True)
        df["Worldwide gross"] = pd.to_numeric(df["Worldwide gross"], errors='coerce')

    # Plot
    fig, ax = plt.subplots(figsize=(10, 6))
    df_sorted = df.sort_values(by="Worldwide gross", ascending=False).head(10)
    df_sorted.plot.bar(x="Title", y="Worldwide gross", ax=ax, legend=False)
    ax.set_title("Top 10 Highest-Grossing Films")
    image_uri = encode_plot_to_base64(fig)

    return {
        "summary": {
            "top_10_movies": df_sorted["Title"].tolist(),
            "total_movies": len(df)
        },
        "plot": image_uri
    }


# 2. Handle CSV Analysis
def handle_csv_analysis(questions: list[str], file):
    df = pd.read_csv(file.file)

    summary = {
        "columns": df.columns.tolist(),
        "shape": df.shape,
        "describe": df.describe().to_dict()
    }

    # Create a sample numeric plot
    numeric_cols = df.select_dtypes(include='number').columns
    if len(numeric_cols) >= 2:
        fig, ax = plt.subplots()
        df.plot.scatter(x=numeric_cols[0], y=numeric_cols[1], ax=ax)
        ax.set_title(f"Scatter: {numeric_cols[0]} vs {numeric_cols[1]}")
        image_uri = encode_plot_to_base64(fig)
    else:
        image_uri = None

    return {
        "summary": summary,
        "plot": image_uri
    }


# 3. Handle High Court Judgement Metadata Analysis
def handle_high_court_analysis(questions: list[str]):
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL parquet; LOAD parquet;")

    query = """
        SELECT court, COUNT(*) as total
        FROM read_parquet(
            's3://indian-high-court-judgments/metadata/parquet/year=*/court=*/bench=*/metadata.parquet?s3_region=ap-south-1'
        )
        WHERE year BETWEEN 2019 AND 2022
        GROUP BY court
        ORDER BY total DESC
    """

    df = con.execute(query).df()

    fig, ax = plt.subplots(figsize=(10, 6))
    df.plot.bar(x="court", y="total", ax=ax)
    ax.set_title("Judgments by High Court (2019–2022)")
    image_uri = encode_plot_to_base64(fig)

    return {
        "summary": df.to_dict(orient="records"),
        "plot": image_uri
    }
