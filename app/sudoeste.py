import io
import logging
import re
import unicodedata

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _normalize_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def _find_column(df: pd.DataFrame, *aliases: str) -> str | None:
    normalized_columns = {_normalize_text(column): column for column in df.columns}

    for alias in aliases:
        key = _normalize_text(alias)
        if key in normalized_columns:
            return normalized_columns[key]

    for alias in aliases:
        key = _normalize_text(alias)
        for normalized_column, original_column in normalized_columns.items():
            if key and key in normalized_column:
                return original_column

    return None


def _require_column(df: pd.DataFrame, *aliases: str) -> str:
    column = _find_column(df, *aliases)
    if column is None:
        raise ValueError(f"Coluna obrigatoria nao encontrada: {aliases[0]}")
    return column


def _ler_tabela_upload(arquivo: bytes) -> pd.DataFrame:
    assinatura = arquivo[:4]
    if assinatura.startswith(b"PK") or assinatura == b"\xd0\xcf\x11\xe0":
        return pd.read_excel(io.BytesIO(arquivo))

    erros_csv = []
    for encoding in ("utf-8-sig", "latin1"):
        try:
            return pd.read_csv(io.BytesIO(arquivo), sep=";", encoding=encoding)
        except Exception as exc:  # pragma: no cover - fallback defensivo
            erros_csv.append(exc)

    try:
        return pd.read_excel(io.BytesIO(arquivo))
    except Exception as exc:
        raise ValueError("Arquivo enviado nao esta em um formato CSV/XLSX valido.") from exc


def _converter_cartao(valor: object) -> object:
    valor_formatado = str(valor).strip().upper()
    if valor_formatado.startswith("CAR") or valor_formatado.startswith("MAS"):
        return "cartão"
    return valor


def _converter_data(valor: object):
    if pd.isna(valor):
        return pd.NaT

    if isinstance(valor, pd.Timestamp):
        return valor

    if isinstance(valor, (int, float, np.integer, np.floating)):
        return pd.to_datetime(valor, unit="D", origin="1899-12-30")

    return pd.to_datetime(valor, dayfirst=True, errors="coerce")


def processar_sudoeste(
    base_excel: bytes,
    pagamentos_excel: bytes,
    relatorio_acionamentos_excel: bytes,
    denodo: bytes,
) -> io.BytesIO:
    logger.info("=" * 10)

    df_base = _ler_tabela_upload(base_excel)
    df_pagamentos = _ler_tabela_upload(pagamentos_excel)
    df_relatorio = _ler_tabela_upload(relatorio_acionamentos_excel)
    _ = _ler_tabela_upload(denodo)

    base_associado_col = _require_column(df_base, "associado")
    base_contrato_col = _require_column(df_base, "n do contrato", "no do contrato")
    base_parcela_col = _require_column(df_base, "n parcela", "no parcela")
    base_vencimento_col = _require_column(df_base, "vencimento")
    base_agencia_col = _require_column(df_base, "agencia")

    pagamento_data_col = _require_column(df_pagamentos, "data")
    pagamento_conta_col = _require_column(df_pagamentos, "conta")
    pagamento_associado_col = _require_column(df_pagamentos, "associado")
    pagamento_titulo_col = _require_column(df_pagamentos, "titulo")
    pagamento_parcela_col = _require_column(df_pagamentos, "parcela")
    pagamento_valor_col = _require_column(df_pagamentos, "valor titulo")
    pagamento_historico_col = _require_column(df_pagamentos, "historico")
    pagamento_cpf_col = _require_column(df_pagamentos, "cpf/cnpj")

    relatorio_data_acionamento_col = _require_column(df_relatorio, "dt acionamento")
    relatorio_nome_col = _require_column(df_relatorio, "nome/razao", "clientenome")

    df_pagamentos[pagamento_titulo_col] = df_pagamentos[pagamento_titulo_col].apply(_converter_cartao)
    df_pagamentos[pagamento_data_col] = df_pagamentos[pagamento_data_col].apply(_converter_data)
    df_base[base_vencimento_col] = df_base[base_vencimento_col].apply(_converter_data)
    df_relatorio[relatorio_data_acionamento_col] = df_relatorio[relatorio_data_acionamento_col].apply(
        _converter_data
    )

    historico_numerico = pd.to_numeric(df_pagamentos[pagamento_historico_col], errors="coerce")
    df_pagamentos[pagamento_historico_col] = historico_numerico
    df_pagamentos = df_pagamentos[
        historico_numerico.isin([1, 2, 3, 4]) | historico_numerico.isna()
    ].copy()

    df_base = df_base.drop_duplicates(
        subset=[base_associado_col, base_contrato_col, base_parcela_col, base_vencimento_col]
    ).copy()

    merge = pd.merge(
        df_pagamentos,
        df_base,
        left_on=[pagamento_associado_col, pagamento_titulo_col, pagamento_parcela_col],
        right_on=[base_associado_col, base_contrato_col, base_parcela_col],
        how="inner",
    )

    if merge.empty:
        raise ValueError("Nenhuma linha correspondente foi encontrada entre pagamentos e base.")

    data_acionamento = (
        pd.merge(
            df_pagamentos[[pagamento_associado_col]].drop_duplicates(),
            df_relatorio[[relatorio_nome_col, relatorio_data_acionamento_col]],
            left_on=pagamento_associado_col,
            right_on=relatorio_nome_col,
            how="left",
        )
        .dropna(subset=[relatorio_data_acionamento_col])
        .groupby(pagamento_associado_col)[relatorio_data_acionamento_col]
        .max()
        .to_dict()
    )

    dados_relatorio = []
    for _, row in merge.iterrows():
        data_pagamento = row.get(pagamento_data_col)
        vencimento = row.get(base_vencimento_col)
        ultimo_acionamento = data_acionamento.get(row.get(pagamento_associado_col))

        dados_relatorio.append(
            {
                "Ag.": row.get(base_agencia_col),
                "Conta": row.get(pagamento_conta_col),
                "Associado": row.get(pagamento_associado_col),
                "CPF/CNPJ": row.get(pagamento_cpf_col),
                "Título": row.get(pagamento_titulo_col),
                "Parcela": row.get(pagamento_parcela_col),
                "Valor Título": row.get(pagamento_valor_col),
                "Histórico": row.get(pagamento_historico_col),
                "Data": data_pagamento.strftime("%d/%m/%Y") if pd.notna(data_pagamento) else "",
                "Atraso": (
                    (data_pagamento.to_pydatetime() - vencimento.to_pydatetime()).days
                    if pd.notna(data_pagamento) and pd.notna(vencimento)
                    else None
                ),
                "% Receita": None,
                "Receita": None,
                "Dt Último Acionamento": (
                    ultimo_acionamento.strftime("%d/%m/%Y") if pd.notna(ultimo_acionamento) else None
                ),
                "Situacao": None,
                "Vencimento Parcela": (
                    vencimento.strftime("%d/%m/%Y") if pd.notna(vencimento) else None
                ),
            }
        )

    dataframe_saida = pd.DataFrame(dados_relatorio).sort_values(
        by=["Associado", "Parcela"], ascending=[True, True]
    )

    diretos = dataframe_saida[dataframe_saida["Dt Último Acionamento"].notna()]
    indiretos = dataframe_saida[dataframe_saida["Dt Último Acionamento"].isna()]

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        diretos.to_excel(writer, sheet_name="DIRETOS", startrow=1, index=False, header=True)
        indiretos.to_excel(writer, sheet_name="INDIRETOS", startrow=1, index=False, header=True)

    output.seek(0)
    return output
