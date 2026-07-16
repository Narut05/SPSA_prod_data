import pandas as pd
import plotly.graph_objects as go

meses = {
    "01":"Ene","02":"Feb","03":"Mar","04":"Abr",
    "05":"May","06":"Jun","07":"Jul","08":"Ago",
    "09":"Sep","10":"Oct","11":"Nov","12":"Dic"
}

def Historico_Productividad_GOS(
    df,
    titulo,
    value1,
    value2,
    anio="Año",
    mes="Mes",
    anio1="2025",
    anio2="2026",
    ymin=None,
    ymax=None,
    y2min=-0.60,
    y2max=0.30,
    archivo_html=None
):

    prod = df.pivot(index=mes, columns=anio, values=value1)
    cump = df.pivot(index=mes, columns=anio, values=value2)

    orden = [f"{i:02d}" for i in range(1, 13)]

    prod = prod.reindex(orden)
    cump = cump.reindex(orden)

    x = [meses[m] for m in orden]

    offset = 0.20
    cump_plot = cump[anio2] + offset

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=x,
            y=prod[anio1],
            mode="lines+markers",
            name=f"{value1} {anio1}",
            line=dict(color="#efa686", width=4),
            marker=dict(
                size=10,
                color="white",
                line=dict(color="#efa686", width=3)
            )
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=prod[anio2],
            mode="lines+markers",
            name=f"{value1} {anio2}",
            line=dict(color="#006077", width=4),
            marker=dict(
                size=10,
                color="white",
                line=dict(color="#006077", width=3)
            )
        )
    )

    fig.add_trace(
        go.Scatter(
            x=x,
            y=cump_plot,
            mode="lines+markers",
            name="% Cump. Meta",
            yaxis="y2",
            line=dict(color="#808080", width=3, dash="dot"),
            marker=dict(
                size=10,
                color="white",
                line=dict(color="#808080", width=3)
            ),
            hovertemplate="%{customdata:.1%}<extra></extra>",
            customdata=cump[anio2]
        )
    )

    def formato_k(v):
        if pd.isna(v):
            return ""
        return f"{v/1000:.0f}K"

    annotations = []

    for xi, yi, v in zip(x, cump_plot, cump[anio2]):

        if pd.isna(v):
            continue

        annotations.append(
            dict(
                x=xi,
                y=yi,
                yref="y2",
                text=f"<b>{v:.1%}</b>",
                showarrow=False,
                yshift=18,
                bgcolor="#06b57c" if v >= 0 else "#f43959",
                bordercolor="#06b57c" if v >= 0 else "#f43959",
                borderpad=4,
                font=dict(color="white", size=10)
            )
        )

    for xi, yi in zip(x, prod[anio2]):

        if pd.isna(yi):
            continue

        annotations.append(
            dict(
                x=xi,
                y=yi,
                yref="y",
                text=f"<b>{formato_k(yi)}</b>",
                showarrow=False,
                yshift=22,
                bgcolor="#006077",
                bordercolor="#006077",
                borderpad=4,
                font=dict(color="white", size=12)
            )
        )

    fig.update_layout(

        annotations=annotations,

        template="plotly_white",

        title=dict(
            text=f"<b>{titulo}</b>",
            x=0.5,
            font=dict(size=24)
        ),

        height=450,
        width=1226,
        hovermode="x unified",

        legend=dict(
            orientation="v",
            x=1.02,
            y=0.98,
            xanchor="left",
            yanchor="top",
            font=dict(size=13)
        ),

        margin=dict(
            l=60,
            r=60,
            t=120,
            b=50
        ),

        xaxis=dict(
            tickfont=dict(size=13)
        ),

        yaxis=dict(
            title=value1,
            showgrid=True,
            range=None if ymin is None or ymax is None else [ymin, ymax],
            gridcolor="#ECECEC",
            zeroline=False,
            tickformat=".0s"
        ),

        yaxis2=dict(
            overlaying="y",
            side="right",
            range=[y2min, y2max],
            # range=[offset - 0.8, offset + 0.1],
            showgrid=False,
            showticklabels=False,
            ticks="",
            zeroline=False
        )
    )

    if archivo_html:
        fig.write_html(
            archivo_html,
            include_plotlyjs="cdn",
            full_html=True,
            config={
                "displayModeBar": False,
                "responsive": True
            }
        )

    return fig


def Historico_Velocidad_GOS(
    df,
    titulo,
    value,
    value_cump,
    anio="Año",
    mes="Mes",
    anio1="2025",
    anio2="2026",
    ymin=None,
    ymax=None,
    y2min=-0.25,
    y2max=0.05,
    archivo_html=None
):

    df = df.copy()

    df[anio] = df[anio].astype(str)
    df[mes] = df[mes].astype(str).str.zfill(2)

    anio1 = str(anio1)
    anio2 = str(anio2)

    orden = [f"{i:02d}" for i in range(1, 13)]

    prod = (
        df.pivot(index=mes, columns=anio, values=value)
        .reindex(orden)
    )

    cump = (
        df.pivot(index=mes, columns=anio, values=value_cump)
        .reindex(orden)
    )

    x = [meses[m] for m in orden]

    fig = go.Figure()

    if anio1 in prod.columns:

        fig.add_trace(
            go.Scatter(
                x=x,
                y=prod[anio1],
                mode="lines+markers",
                name=f"{value} {anio1}",
                line=dict(color="#efa686", width=4),
                marker=dict(
                    size=10,
                    color="white",
                    line=dict(color="#efa686", width=3)
                )
            )
        )

    if anio2 in prod.columns:

        fig.add_trace(
            go.Scatter(
                x=x,
                y=prod[anio2],
                mode="lines+markers",
                name=f"{value} {anio2}",
                line=dict(color="#006077", width=4),
                marker=dict(
                    size=10,
                    color="white",
                    line=dict(color="#006077", width=3)
                )
            )
        )

    if anio2 in cump.columns:

        fig.add_trace(
            go.Scatter(
                x=x,
                y=cump[anio2],
                mode="lines+markers",
                name="% Cump. Meta",
                yaxis="y2",
                line=dict(
                    color="#808080",
                    width=3,
                    dash="dot"
                ),
                marker=dict(
                    size=9,
                    color="white",
                    line=dict(color="#808080", width=3)
                ),
                hovertemplate="%{y:.1%}<extra></extra>"
            )
        )

    annotations = []

    if anio2 in prod.columns:

        for xi, yi in zip(x, prod[anio2]):

            if pd.isna(yi):
                continue

            annotations.append(
                dict(
                    x=xi,
                    y=yi,
                    text=f"<b>{yi:.2f}</b>",
                    showarrow=False,
                    yshift=22,
                    bgcolor="#006077",
                    bordercolor="#006077",
                    borderpad=4,
                    font=dict(
                        color="white",
                        size=12
                    )
                )
            )

    if anio2 in cump.columns:

        for xi, yi in zip(x, cump[anio2]):

            if pd.isna(yi):
                continue

            annotations.append(
                dict(
                    x=xi,
                    y=yi,
                    yref="y2",
                    text=f"<b>{yi:.1%}</b>",
                    showarrow=False,
                    yshift=18,
                    bgcolor="#06b57c" if yi >= 0 else "#f43959",
                    bordercolor="#06b57c" if yi >= 0 else "#f43959",
                    borderpad=4,
                    font=dict(
                        color="white",
                        size=10
                    )
                )
            )

    fig.update_layout(

        template="plotly_white",

        annotations=annotations,

        title=dict(
            text=f"<b>{titulo}</b>",
            x=0.5,
            font=dict(size=24)
        ),

        height=450,
        width=1226,

        hovermode="x unified",

        legend=dict(
            orientation="v",
            x=1.02,
            y=0.98,
            xanchor="left",
            yanchor="top",
            font=dict(size=13)
        ),

        margin=dict(
            l=60,
            r=60,
            t=120,
            b=50
        ),

        xaxis=dict(
            tickfont=dict(size=13)
        ),

        yaxis=dict(
            title=value,
            range=None if ymin is None or ymax is None else [ymin, ymax],
            tickformat=".2f",
            showgrid=True,
            gridcolor="#ECECEC",
            zeroline=False
        ),

        yaxis2=dict(
            overlaying="y",
            side="right",
            range=[y2min, y2max],
            showgrid=False,
            showticklabels=False,
            ticks="",
            zeroline=False
        )
    )

    if archivo_html:

        fig.write_html(
            archivo_html,
            include_plotlyjs="cdn",
            full_html=True,
            config={
                "displayModeBar": False,
                "responsive": True
            }
        )

    return fig


