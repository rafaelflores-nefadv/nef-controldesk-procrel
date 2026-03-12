from pathlib import Path

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from .planalto import processar_planalto
from .sudoeste import processar_sudoeste
from .sudoeste_direto import processar_sudoeste_direto
from .sudoeste_indireto import processar_sudoeste_indireto

app = FastAPI()

EXCEL_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
STATIC_DIR = Path(__file__).resolve().parent.parent / "static"

# Para frontend e backend em origens diferentes no futuro:
# from fastapi.middleware.cors import CORSMiddleware
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://127.0.0.1:3000"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")

@app.post("/planalto")
async def planalto(
    recebimento: UploadFile = File(...),
    pagamento: UploadFile = File(...)
):
    try:
        recebimento_bytes = await recebimento.read()
        pagamento_bytes = await pagamento.read()

        resultado = processar_planalto(recebimento_bytes, pagamento_bytes)

        return StreamingResponse(
            resultado,
            media_type=EXCEL_MEDIA_TYPE,
            headers={"Content-Disposition": "attachment; filename=planalto_processado.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")


@app.post("/sudoeste")
async def sudoeste(
    base: UploadFile = File(...),
    recebimento: UploadFile = File(...),
    denodo: UploadFile = File(...)
):
    try:
        base_bytes = await base.read()
        recebimento_bytes = await recebimento.read()
        denodo_bytes = await denodo.read()

        resultado = processar_sudoeste(
            base_bytes,
            recebimento_bytes,
            denodo_bytes
        )

        return StreamingResponse(
            resultado,
            media_type=EXCEL_MEDIA_TYPE,
            headers={"Content-Disposition": "attachment; filename=sudoeste_inicial_processado.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")


@app.post("/sudoeste-direto")
async def sudoeste_direto(
    processada: UploadFile = File(...),
    direta: UploadFile = File(...)
):
    try:
        processada_bytes = await processada.read()
        direta_bytes = await direta.read()

        resultado = processar_sudoeste_direto(processada_bytes, direta_bytes)

        return StreamingResponse(
            resultado,
            media_type=EXCEL_MEDIA_TYPE,
            headers={"Content-Disposition": "attachment; filename=sudoeste_direto_processado.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")


@app.post("/sudoeste-indireto")
async def sudoeste_indireto(
    processada: UploadFile = File(...),
    indireto: UploadFile = File(...)
):
    try:
        processada_bytes = await processada.read()
        indireto_bytes = await indireto.read()

        resultado = processar_sudoeste_indireto(processada_bytes, indireto_bytes)

        return StreamingResponse(
            resultado,
            media_type=EXCEL_MEDIA_TYPE,
            headers={"Content-Disposition": "attachment; filename=sudoeste_indireto_processado.xlsx"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")
