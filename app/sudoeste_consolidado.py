import io

import pandas as pd

from .sudoeste_direto import processar_sudoeste_direto_frames
from .sudoeste_indireto import processar_sudoeste_indireto_frames


def _exportar_sudoeste_consolidado(
    direto_df: pd.DataFrame,
    indireto_df: pd.DataFrame,
) -> io.BytesIO:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        direto_df.to_excel(writer, sheet_name="Direto", index=False)
        indireto_df.to_excel(writer, sheet_name="Indireto", index=False)
    output.seek(0)
    return output


def processar_sudoeste_consolidado(
    processada_excel: bytes,
    direta_excel: bytes,
    indireto_excel: bytes,
) -> io.BytesIO:
    direto_df = processar_sudoeste_direto_frames(processada_excel, direta_excel)
    indireto_df = processar_sudoeste_indireto_frames(processada_excel, indireto_excel)
    return _exportar_sudoeste_consolidado(direto_df, indireto_df)
