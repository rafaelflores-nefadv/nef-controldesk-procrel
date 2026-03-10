const FORM_CONFIG = {
    planalto: {
        endpoint: "/planalto",
        filename: "planalto_processado.xlsx",
        requiredCount: 2,
        fields: [
            {
                name: "recebimento",
                label: "Arquivo de recebimento",
                hint: "Envie o XLSX de recebimento do Planalto."
            },
            {
                name: "pagamento",
                label: "Arquivo de pagamento",
                hint: "Envie o XLSX de remessa ou pagamento."
            }
        ]
    },
    sudoeste: {
        endpoint: "/sudoeste",
        filename: "sudoeste_processado.xlsx",
        requiredCount: 4,
        fields: [
            {
                name: "base",
                label: "Arquivo base",
                hint: "Aceita CSV ou XLSX."
            },
            {
                name: "pagamentos",
                label: "Arquivo de pagamentos",
                hint: "Aceita CSV ou XLSX."
            },
            {
                name: "relatorio",
                label: "Relatório de acionamentos",
                hint: "Envie o XLSX do relatório."
            },
            {
                name: "denodo",
                label: "Arquivo Denodo",
                hint: "Envie o XLSX de apoio."
            }
        ]
    }
};

const form = document.getElementById("upload-form");
const tipoSelect = document.getElementById("tipo");
const inputsContainer = document.getElementById("inputs-container");
const requirementsText = document.getElementById("requirements-text");
const submitButton = document.getElementById("submit-button");
const messageBox = document.getElementById("message");

function renderFields() {
    const tipo = tipoSelect.value;
    const config = FORM_CONFIG[tipo];

    requirementsText.textContent = `${config.requiredCount} arquivo(s): ${config.fields
        .map((field) => field.label)
        .join(", ")}.`;

    inputsContainer.innerHTML = config.fields
        .map(
            (field) => `
                <div class="file-field">
                    <label for="${field.name}">
                        ${field.label}
                        <span class="required-badge">Obrigatório</span>
                    </label>
                    <input id="${field.name}" type="file" name="${field.name}" required>
                    <span class="hint">${field.hint}</span>
                </div>
            `
        )
        .join("");

    clearMessage();
}

function setMessage(text, type = "info") {
    messageBox.textContent = text;
    messageBox.className = `message is-visible ${type === "error" ? "is-error" : "is-info"}`;
}

function clearMessage() {
    messageBox.textContent = "";
    messageBox.className = "message";
}

function setLoading(isLoading, text) {
    submitButton.disabled = isLoading;
    submitButton.textContent = isLoading ? text : "Enviar arquivos";
}

function buildFormData(config) {
    const formData = new FormData();
    const missing = [];

    for (const field of config.fields) {
        const input = document.getElementById(field.name);
        const file = input?.files?.[0];

        if (!file) {
            missing.push(field.label);
            continue;
        }

        formData.append(field.name, file);
    }

    if (missing.length > 0) {
        throw new Error(`Envie todos os arquivos obrigatórios: ${missing.join(", ")}.`);
    }

    return formData;
}

function getFilenameFromDisposition(contentDisposition, fallbackName) {
    if (!contentDisposition) {
        return fallbackName;
    }

    const utf8Match = contentDisposition.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf8Match?.[1]) {
        return decodeURIComponent(utf8Match[1]);
    }

    const simpleMatch = contentDisposition.match(/filename="?([^"]+)"?/i);
    if (simpleMatch?.[1]) {
        return simpleMatch[1];
    }

    return fallbackName;
}

async function extractErrorMessage(response) {
    try {
        const data = await response.json();
        if (typeof data?.detail === "string" && data.detail.trim()) {
            return data.detail;
        }
    } catch (error) {
        return `Falha no envio: ${response.status} ${response.statusText}`.trim();
    }

    return `Falha no envio: ${response.status} ${response.statusText}`.trim();
}

async function handleSubmit(event) {
    event.preventDefault();

    const tipo = tipoSelect.value;
    const config = FORM_CONFIG[tipo];

    try {
        setLoading(true, "Enviando...");
        setMessage("Validando arquivos...", "info");

        const formData = buildFormData(config);

        setLoading(true, "Processando...");
        setMessage("Arquivos enviados. Processando relatório...", "info");

        const response = await fetch(config.endpoint, {
            method: "POST",
            body: formData
        });

        if (!response.ok) {
            throw new Error(await extractErrorMessage(response));
        }

        const blob = await response.blob();
        const filename = getFilenameFromDisposition(
            response.headers.get("Content-Disposition"),
            config.filename
        );

        const url = window.URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = url;
        anchor.download = filename;
        document.body.appendChild(anchor);
        anchor.click();
        anchor.remove();
        window.URL.revokeObjectURL(url);

        setMessage(`Concluído. Download iniciado: ${filename}`, "info");
    } catch (error) {
        setMessage(error.message || "Erro inesperado ao processar os arquivos.", "error");
    } finally {
        setLoading(false);
    }
}

tipoSelect.addEventListener("change", renderFields);
form.addEventListener("submit", handleSubmit);

renderFields();
