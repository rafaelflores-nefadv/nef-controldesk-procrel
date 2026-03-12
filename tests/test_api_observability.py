import io
import logging
import re
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.api import app
from app.logging_utils import log_info


class ApiObservabilityTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def _extract_request_id_from_logs(self, logs: list[str]) -> str | None:
        match_pattern = re.compile(r'"request_id":\s*"([a-f0-9]{32})"')
        for message in logs:
            matched = match_pattern.search(message)
            if matched:
                return matched.group(1)
        return None

    def test_retorna_422_com_mensagem_amigavel_quando_falta_campo_obrigatorio(self):
        files = {
            "base": ("base.xlsx", b"base-content", "application/octet-stream"),
            "denodo": ("denodo.xlsx", b"denodo-content", "application/octet-stream"),
        }

        with self.assertLogs("app.api", level="WARNING") as captured_logs:
            response = self.client.post("/sudoeste", files=files)

        self.assertEqual(response.status_code, 422)
        self.assertIn(
            "Fluxo sudoeste-inicial recebeu campos incompletos. Faltando: recebimento.",
            response.json()["detail"],
        )
        request_id = self._extract_request_id_from_logs(captured_logs.output)
        self.assertIsNotNone(request_id)
        self.assertIn(
            f"request_id={request_id}",
            response.json()["detail"],
        )
        self.assertTrue(
            any("Campos ausentes na requisicao" in message for message in captured_logs.output)
        )
        self.assertTrue(any('"request_id":' in message for message in captured_logs.output))

    def test_fluxo_sudoeste_nao_aceita_chave_antiga_relatorio(self):
        files = {
            "base": ("base.xlsx", b"base-content", "application/octet-stream"),
            "relatorio": ("relatorio.xlsx", b"relatorio-content", "application/octet-stream"),
            "denodo": ("denodo.xlsx", b"denodo-content", "application/octet-stream"),
        }

        with self.assertLogs("app.api", level="WARNING") as captured_logs:
            response = self.client.post("/sudoeste", files=files)

        self.assertEqual(response.status_code, 422)
        self.assertIn("Faltando: recebimento", response.json()["detail"])
        self.assertTrue(
            any("Chaves multipart inesperadas detectadas" in message for message in captured_logs.output)
        )
        self.assertTrue(
            any('"chaves_inesperadas": ["relatorio"]' in message for message in captured_logs.output)
        )

    def test_dispara_logs_do_endpoint_em_requisicao_valida(self):
        files = {
            "base": ("base.xlsx", b"base-content", "application/octet-stream"),
            "recebimento": ("recebimento.xlsx", b"recebimento-content", "application/octet-stream"),
            "denodo": ("denodo.xlsx", b"denodo-content", "application/octet-stream"),
        }

        with patch("app.api.processar_sudoeste", return_value=io.BytesIO(b"xlsx")):
            with self.assertLogs("app.api", level="INFO") as captured_logs:
                response = self.client.post("/sudoeste", files=files)

        self.assertEqual(response.status_code, 200)
        self.assertIn(
            "attachment; filename=sudoeste_inicial_processado.xlsx",
            response.headers.get("content-disposition", ""),
        )
        self.assertRegex(response.headers.get("x-request-id", ""), r"^[a-f0-9]{32}$")
        self.assertTrue(
            any("Recebida requisicao de processamento" in message for message in captured_logs.output)
        )
        self.assertTrue(
            any("Validacao de campos obrigatorios concluida" in message for message in captured_logs.output)
        )
        self.assertTrue(
            any("Processamento concluido com sucesso" in message for message in captured_logs.output)
        )
        self.assertTrue(any('"request_id":' in message for message in captured_logs.output))

    def test_erro_de_processamento_retorna_mensagem_segura_e_loga_exception(self):
        files = {
            "base": ("base.xlsx", b"base-content", "application/octet-stream"),
            "recebimento": ("recebimento.xlsx", b"recebimento-content", "application/octet-stream"),
            "denodo": ("denodo.xlsx", b"denodo-content", "application/octet-stream"),
        }

        with patch("app.api.processar_sudoeste", side_effect=RuntimeError("falha-interna")):
            with self.assertLogs("app.api", level="ERROR") as captured_logs:
                response = self.client.post("/sudoeste", files=files)

        self.assertEqual(response.status_code, 500)
        self.assertIn(
            "Erro interno ao processar fluxo sudoeste-inicial. Verifique os arquivos enviados.",
            response.json()["detail"],
        )
        self.assertNotIn("falha-interna", response.text)
        request_id = self._extract_request_id_from_logs(captured_logs.output)
        self.assertIsNotNone(request_id)
        self.assertIn(f"request_id={request_id}", response.json()["detail"])
        self.assertTrue(any("Erro ao processar fluxo" in message for message in captured_logs.output))
        self.assertTrue(any('"request_id":' in message for message in captured_logs.output))

    def test_request_id_e_propagado_para_logs_do_fluxo(self):
        files = {
            "base": ("base.xlsx", b"base-content", "application/octet-stream"),
            "recebimento": ("recebimento.xlsx", b"recebimento-content", "application/octet-stream"),
            "denodo": ("denodo.xlsx", b"denodo-content", "application/octet-stream"),
        }

        def _processor_com_log_de_fluxo(**_kwargs):
            flow_logger = logging.getLogger("app.sudoeste")
            log_info(flow_logger, "Fluxo de teste executado")
            return io.BytesIO(b"xlsx")

        with patch("app.api.processar_sudoeste", side_effect=_processor_com_log_de_fluxo):
            with self.assertLogs(level="INFO") as captured_logs:
                response = self.client.post("/sudoeste", files=files)

        self.assertEqual(response.status_code, 200)
        request_id = self._extract_request_id_from_logs(captured_logs.output)
        self.assertIsNotNone(request_id)

        api_log_with_request_id = any(
            "app.api" in message
            and "Recebida requisicao de processamento" in message
            and f'"request_id": "{request_id}"' in message
            for message in captured_logs.output
        )
        flow_log_with_same_request_id = any(
            "app.sudoeste" in message
            and "Fluxo de teste executado" in message
            and f'"request_id": "{request_id}"' in message
            for message in captured_logs.output
        )
        self.assertTrue(api_log_with_request_id)
        self.assertTrue(flow_log_with_same_request_id)


if __name__ == "__main__":
    unittest.main()
