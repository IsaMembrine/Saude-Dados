import streamlit as st
import pandas as pd
import plotly.express as px

# 🚨 Leitura dos dados salvos em CSV
monthy_selecionado = pd.read_csv("monthy_selecionado.csv")
df_corr = pd.read_csv("df_corr.csv")

# 🧼 Tratamento de datas
monthy_selecionado["Month"] = pd.to_datetime(monthy_selecionado["Month"], errors='coerce')
monthy_selecionado.dropna(subset=["Month"], inplace=True)
monthy_selecionado["Month_Str"] = monthy_selecionado["Month"].dt.strftime('%Y-%m')

df_corr["Month_sort"] = pd.to_datetime(df_corr["Month"])
df_corr.sort_values("Month_sort", inplace=True)

# 🎨 Interface do Streamlit
st.set_page_config(page_title="Painéis Piezômetros", layout="wide")
st.title("📊 Dashboards dos Piezômetros")

aba1, aba2 = st.tabs(["Porcentagem de Entrega", "Correlação Mensal"])

# Aba 1: Presença
with aba1:
    node1 = st.selectbox("Selecione um Piezômetro", sorted(monthy_selecionado["Node_ID"].unique()))
    df1 = monthy_selecionado[monthy_selecionado["Node_ID"] == node1]

    fig1 = px.bar(df1, x="Month_Str", y="Monthly_Attendance_Percentage",
                  title=f"Presença Mensal — {node1}",
                  labels={"Month_Str": "Mês", "Monthly_Attendance_Percentage": "Presença (%)"},
                  color_discrete_sequence=["royalblue"])
    fig1.update_layout(xaxis=dict(type="category", tickangle=-45))
    st.plotly_chart(fig1, use_container_width=True)

# Aba 2: Correlação
with aba2:
    node2 = st.selectbox("Selecione um Piezômetro para Correlação", sorted(df_corr["Node_ID"].unique()))
    df2 = df_corr[df_corr["Node_ID"] == node2]

    fig2 = px.bar(df2, x="Month", y="Correlation",
                  title=f"Correlação Mensal para o Nó {node2}",
                  labels={"Month": "Mês", "Correlation": "Coeficiente de Correlação"},
                  color_discrete_sequence=["indianred"])
    fig2.add_shape(type="line", x0=0, x1=1, y0=-0.75, y1=-0.75,
                   xref="paper", yref="y", line=dict(color="green", dash="dash", width=2))
    fig2.update_layout(xaxis=dict(type="category", tickangle=-45), yaxis=dict(range=[-1, 1]))
    st.plotly_chart(fig2, use_container_width=True)
