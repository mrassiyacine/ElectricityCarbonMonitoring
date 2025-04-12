import altair as alt
import duckdb
import pandas as pd
import streamlit as st

flag_map = {"FR": "🇫🇷", "DE": "🇩🇪"}
st.set_page_config(
    page_title="Electricity Cleanliness Report",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

query_avg_carbon_intensity = """
WITH daily_data AS (
    SELECT
      zone,
      carbonIntensity,
    FROM electricity_maps_data
    WHERE zone IN ('FR', 'DE') -- France, Germany
      AND datetime >= NOW() - INTERVAL 1 DAY
  )
  SELECT
    zone,
    AVG(carbonIntensity) AS avg_carbon_intensity,
  FROM daily_data
  GROUP BY zone;
"""
with duckdb.connect("data/processed/db.duckdb") as con:
    df_avg_carbon_intensity = con.execute(query_avg_carbon_intensity).fetchdf()

TODAY = pd.to_datetime("now").date()
st.title(f"Electricity Cleanliness Report for {TODAY}")
st.caption("How clean was our electricity? Data from Electricity Maps")

st.subheader("📉 Carbon Intensity (gCO₂eq/kWh) for Today")
st.info(
    "🌍 Carbon intensity is a direct signal of how green a country’s electricity mix is. LOWER = CLEANER."
)
carbon_bar_chart = (
    alt.Chart(df_avg_carbon_intensity)
    .mark_bar()
    .encode(
        x="zone:N",
        y="avg_carbon_intensity:Q",
        tooltip=["zone:N", "avg_carbon_intensity:Q"],
    )
    .properties(title="Today Carbon Intensity 🇫🇷 vs 🇩🇪")
)
st.altair_chart(carbon_bar_chart, use_container_width=True)

query_carbon_intensity_over_the_day = """
with daily_data as (
    select
        zone,
        date_trunc('hour', datetime) as hour,
        carbonIntensity
    from electricity_maps_data
    where zone in ('FR', 'DE')
        and datetime >= now() - interval 1 DAY
)
select
    zone,
    hour,
    carbonIntensity
from daily_data
order by zone, hour;
"""

with duckdb.connect("data/processed/db.duckdb") as con:
    df_carbon_intensity_over_the_day = con.execute(
        query_carbon_intensity_over_the_day
    ).fetchdf()

df_france = df_carbon_intensity_over_the_day[
    df_carbon_intensity_over_the_day["zone"] == "FR"
]
df_germany = df_carbon_intensity_over_the_day[
    df_carbon_intensity_over_the_day["zone"] == "DE"
]

carbon_line_chart_france = (
    alt.Chart(df_france)
    .mark_line()
    .encode(x="hour:T", y="carbonIntensity:Q", tooltip=["hour:T", "carbonIntensity:Q"])
    .properties(
        title="Carbon Intensity Over the Day in France 🇫🇷", width=800, height=400
    )
    .interactive()
)

st.altair_chart(carbon_line_chart_france, use_container_width=True)

carbon_line_chart_germany = (
    alt.Chart(df_germany)
    .mark_line()
    .encode(
        x="hour:T",
        y="carbonIntensity:Q",
        tooltip=["hour:T", "carbonIntensity:Q"],
    )
    .properties(
        title="Carbon Intensity Over the Day in Germany 🇩🇪", width=800, height=400
    )
    .interactive()
)

st.altair_chart(carbon_line_chart_germany, use_container_width=True)
st.subheader("🌱 Fossil-Free & Renewable Energy (%)")

st.info(
    """
**🟢 Fossil-Free Percentage**  
Electricity not produced by fossil fuels — includes nuclear, wind, solar, hydro.

**✅ Renewable Percentage**  
Only renewable sources — wind, solar, hydro (excludes nuclear).
"""
)


query_fossil_free_renewable_energy = """
WITH daily_data AS (
  SELECT
    zone,
    DATE_TRUNC('day', datetime) AS day,
    fossilFreePercentage,
    renewablePercentage
  FROM electricity_maps_data
  WHERE zone IN ('FR', 'DE')
    AND datetime >= now() - interval 1 DAY
)
SELECT
  zone,
  AVG(fossilFreePercentage) AS avg_fossil_free,
  AVG(renewablePercentage) AS avg_renewable
FROM daily_data
GROUP BY zone
ORDER BY zone;
"""
with duckdb.connect("data/processed/db.duckdb") as con:
    df_fossil_free_renewable_energy = con.execute(
        query_fossil_free_renewable_energy
    ).fetchdf()

fossil_free_bar_chart = (
    alt.Chart(df_fossil_free_renewable_energy)
    .mark_bar()
    .encode(
        x="zone:N",
        y="avg_fossil_free:Q",
        color=alt.value("green"),
        tooltip=["zone:N", "avg_fossil_free:Q"],
    )
    .properties(title="Fossil-Free Energy (%)")
)
renewable_bar_chart = (
    alt.Chart(df_fossil_free_renewable_energy)
    .mark_bar()
    .encode(
        x="zone:N",
        y="avg_renewable:Q",
        color=alt.value("blue"),
        tooltip=["zone:N", "avg_renewable:Q"],
    )
    .properties(title="Renewable Energy (%)")
)
st.altair_chart(fossil_free_bar_chart, use_container_width=True)
st.altair_chart(renewable_bar_chart, use_container_width=True)

query_power_production = """
WITH daily_data AS (
  SELECT
    zone,
    SUM("powerProductionBreakdown.nuclear") AS nuclear,
    SUM("powerProductionBreakdown.coal") AS coal,
    SUM("powerProductionBreakdown.gas") AS gas,
    SUM("powerProductionBreakdown.wind") AS wind,
    SUM("powerProductionBreakdown.solar") AS solar,
    SUM("powerProductionBreakdown.hydro") AS hydro
  FROM electricity_maps_data
  WHERE zone IN ('FR', 'DE')
    AND datetime >= NOW() - INTERVAL 1 DAY
  GROUP BY zone
)
SELECT * FROM daily_data;
"""

with duckdb.connect("data/processed/db.duckdb") as con:
    df_power_production = con.execute(query_power_production).fetchdf()

df_melted = df_power_production.melt(
    id_vars=["zone"], var_name="Source", value_name="MW"
)

emoji_map = {
    "nuclear": "☢️ Nuclear",
    "coal": "🏭 Coal",
    "gas": "🔥 Gas",
    "wind": "🌬️ Wind",
    "solar": "☀️ Solar",
    "hydro": "💧 Hydro",
}
df_melted["Source"] = df_melted["Source"].map(emoji_map)

source_order = ["☢️ Nuclear", "🏭 Coal", "🔥 Gas", "🌬️ Wind", "☀️ Solar", "💧 Hydro"]
df_melted["Source"] = pd.Categorical(
    df_melted["Source"], categories=source_order, ordered=True
)

color_scheme = alt.Scale(
    domain=source_order,
    range=["#FF5733", "#7D3C98", "#F39C12", "#3498DB", "#F4D03F", "#1ABC9C"],
)

st.subheader("🔋 Today's Power Production by Source")
st.info(
    """
Each bar shows **how much electricity (in MWh)** was generated today by each energy source, per country.

**Energy Source Emojis:**
- ☢️ Nuclear — low-carbon, not renewable  
- 🏭 Coal — fossil fuel  
- 🔥 Gas — fossil fuel  
- 🌬️ Wind — renewable  
- ☀️ Solar — renewable  
- 💧 Hydro — renewable
"""
)

for country in df_melted["zone"].unique():
    df_country = df_melted[df_melted["zone"] == country]
    flag = flag_map.get(country, "")
    bar_chart = (
        alt.Chart(df_country)
        .mark_bar()
        .encode(
            x=alt.X("Source:N", sort=source_order, title="Energy Source"),
            y=alt.Y("MW:Q", title="Total Power (MW)"),
            color=alt.Color(
                "Source:N", scale=color_scheme, legend=alt.Legend(title="Energy Source")
            ),
            tooltip=["zone:N", "Source:N", "MW:Q"],
        )
        .properties(
            title=f"Power Production in {country} {flag}", width=600, height=400
        )
        .configure_axis(labelFontSize=12, titleFontSize=14)
        .configure_legend(titleFontSize=14, labelFontSize=12)
    )
    st.altair_chart(bar_chart, use_container_width=True)
