import json
import shutil
import logging as log
from copy import deepcopy
from pathlib import Path
from datetime import date, datetime
from typing import Any

import requests
from pypdf import PdfReader


log.basicConfig(
    level=log.INFO,
    filename="./app.log",
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8"
)


def init_config() -> dict[str, Any]:
    try:
        with open("config.json", "r", encoding="utf-8") as file:
            dict_config: dict[str, Any] = json.load(file)
        log.info("Arquivo de configuração encontrado.")

    except FileNotFoundError:
        log.error("Arquivo de configuração não encontrado.")
        raise
    except Exception as e:
        log.error(f"Erro inesperado: {e}")
        raise

    return dict_config


def obter_competencia_atual() -> tuple[int, int]:
    hoje = date.today()
    dia = hoje.day
    mes = hoje.month
    ano = hoje.year

    if dia >= 15:
        if mes == 12:
            mes = 1
            ano += 1
        else:
            mes += 1
    # senão, mes permanece o mesmo

    log.info(f"Competência -> Mês: {mes}, Ano: {ano}")
    return mes, ano


def get_contratos_api(solicitado: str, url_get: str, headers: dict[str, str]) -> list[dict]:
    """Carrega dados de todos os contratos via API Superlógica."""

    log.info("Carregando dados de contratos.")

    BASE_URL = f"{url_get}{solicitado}"
    PARAMS = {"itensPorPagina": 150, "pagina": 1}

    todos_os_dados = []

    while True:
        response = requests.get(BASE_URL, headers=headers, params=PARAMS)

        if response.status_code != 200:
            log.error(f"Erro na requisição: {response.status_code}")
            break

        data = response.json()

        if not data or len(data["data"]) < PARAMS["itensPorPagina"]:
            # Se a resposta for vazia ou menor que o limite, é a última página
            todos_os_dados.extend(data["data"])
            break

        todos_os_dados.extend(data["data"])  # Adiciona à lista principal
        PARAMS["pagina"] += 1  # Avança para a próxima página

    log.info(f"Total de contratos salvos: {len(todos_os_dados)}")

    if not todos_os_dados:
        log.error("Nenhum contrato encontrado.")
        raise Exception("Nenhum contrato encontrado.")

    return todos_os_dados


def relacionar_codigo_e_id_contratos(lista_contratos: list[dict]) -> dict[str, str]:
    """Cria um dicionário que relaciona código do contrato com id do contrato no Superlógica."""

    id_contratos = {}

    for contrato in lista_contratos:
        cod_contrato = contrato["codigo_contrato"].split("/")[0]
        id_contrato = contrato["id_contrato_con"]

        id_contratos[cod_contrato] = id_contrato

    with open("data/relação id contrato.json", "w", encoding="utf-8") as file:
        json.dump(id_contratos, file, indent=4, ensure_ascii=False)

    log.info("Relação código e id de contratos criada.")
    return id_contratos


def listar_arquivos_pdf(diretorio: str) -> list[Path]:
    caminho_path = Path(diretorio)

    if not caminho_path.exists():
        log.error("Caminho do diretório não encontrado.")
        raise FileNotFoundError("Caminho do diretório não encontrado.")

    if not caminho_path.is_dir():
        log.error("Caminho não é um diretório.")
        raise NotADirectoryError("Caminho não é um diretório.")

    # Cria uma lista com os paths dos arquivos contidos no diretório
    pdfs = list(caminho_path.glob("*.pdf"))

    if not pdfs:
        log.error("Nenhum arquivo encontrado no diretório.")
        raise ValueError("Nenhum arquivo encontrado no diretório.")

    return pdfs


def extrair_dados_pdf(caminho_pdf: Path, mes_lancamento: int) -> tuple[str, str, str]:
    """Extrai do pdf a data de vencimento, código de barras e valor total."""

    reader = PdfReader(caminho_pdf)

    qtd_paginas = reader.get_num_pages()

    corretor_indice = 12 - qtd_paginas

    pagina = reader.pages[int(mes_lancamento-corretor_indice)]
    texto_pagina = pagina.extract_text().split("\n")

    data_vencimento = texto_pagina[12]
    cod_barras = texto_pagina[33].replace(".", "").replace(" ", "")
    valor_total = texto_pagina[31].replace(".", "").replace(",", ".")

    log.info("Dados extraídos do pdf.")
    return data_vencimento, cod_barras, valor_total


def formatar_data_vencimento(data_vencimento_bruta: str) -> str:
    """Formata a data de vencimento no formato solicitado pela API."""

    # transforma em objeto datetime
    obj_data = datetime.strptime(data_vencimento_bruta, "%d/%m/%Y")

    data_formatada = obj_data.strftime("%m/%d/%Y")  # converte para mm/dd/aaaa

    return data_formatada


def get_despesas_iptu_api(solicitado: str, url_get: str, headers: dict, payload: dict) -> list[dict]:
    """Carrega, de um contrato, todas as despesas referentes a IPTU"""

    log.info("Carregando despesas IPTU do contrato")

    BASE_URL = f"{url_get}{solicitado}"
    PARAMS = payload

    todos_os_dados = []
    while True:
        response = requests.get(BASE_URL, headers=headers, params=PARAMS)

        if response.status_code != 200:
            log.error(f"Erro na requisição: {response.status_code}")
            break

        data = response.json()

        if not data or len(data["data"]) < PARAMS["itensPorPagina"]:
            # Se a resposta for vazia ou menor que o limite, é a última página
            todos_os_dados.extend(data["data"])
            break

        todos_os_dados.extend(data["data"])  # Adiciona à lista principal
        PARAMS["pagina"] += 1  # Avança para a próxima página

    log.info(f"Total de despesas IPTU encontradas: {len(todos_os_dados)}")

    if not todos_os_dados:
        raise ValueError

    return todos_os_dados


def get_info_despesa(url_info: str, headers: dict, payload: dict) -> dict[Any, Any]:
    """Carrega os dados da despesa para serem aproveitados como parâmetros para o PUT."""

    log.info("Carregando dados da despesa selecionada.")

    BASE_URL = f"{url_info}"
    PARAMS = payload

    response = requests.get(BASE_URL, headers=headers, params=PARAMS)

    if response.status_code != 200:
        log.error(
            f"Erro na requisição para obtenção dos parâmetros: {response.status_code}")
        renomear_e_mover_arquivo(
            pdf, "Erro na requisição para obtenção dos parâmetros", caminho_iptu_erro)

    data = response.json()

    dict_info_desp: dict[Any, Any] = data["data"]

    log.info("Dados da despesa carregados com sucesso.")
    return dict_info_desp


def alterar_valor_despesa_api_sl(url_put: str, headers: dict, info_despesa: dict, codigo_barras: str, data_venc_formatada: str) -> None:
    """Envia a requisição PUT para lançar e/ou alterar o código de barras e a data de vencimento na despesa."""

    comp = info_despesa["composicoes"][0]

    PAYLOAD = {
        "ID_LANCTOPROGREALIZADO_LPR": "",
        "ID_LANCAMENTO_IMOD": "",
        "ID_LANCAMENTO_IMODM": "",
        "DT_VENCIMENTO": data_venc_formatada,
        "DT_LIQUIDACAO_MOV": "",
        "VL_VALOR_IMOD": f"-{info_despesa['vl_valor']}",
        "NM_NUMERO_CH": 0,
        "ID_DEBITO_IMOD": "",
        "ID_RECEBIMENTO_RECB": "",
        "ID_REPASSE_REP": "",
        "ID_FORMAPAGAMENTO_IMOD": info_despesa["id_formapagamento"],
        "FL_MANTERCHAVE": 1,
        "NM_TAGLIQUIDACAO": "",
        "NM_TAGCRIACAO": "",
        "DT_ATUAL_COMPETENCIA": info_despesa["dt_competencia"],
        "FL_DIFERENCA": 0,
        "ID_TERCEIRO_FAV": info_despesa["id_terceiro_fav"],
        "FL_TIPODESPESA": 2,
        "VL_TOTAL": info_despesa["vl_valor"],
        "ID_LANCAMENTO": info_despesa["id_lancamento"],
        "ID_PRODUTO_PRD": info_despesa["id_produto_prd"],
        "FL_STATUS_MOV": info_despesa["fl_status"],
        "ID_CREDITO": info_despesa["id_credito"],
        "DT_REFERENCIA": info_despesa["dt_referencia"],
        "ID_CONTRATO_CON": info_despesa["id_contrato_con"],
        "ID_FORMAPAGAMENTO": info_despesa["id_formapagamento"],
        "ID_IMOVEL_IMO": info_despesa["id_imovel_imo"],
        "DT_COMPETENCIA": info_despesa["dt_competencia"],
        "ID_CONTABANCO_MOV": info_despesa["id_contabanco_mov"],
        "FL_CONCILIADO": info_despesa["fl_conciliado"],
        "FL_ALTERAR_COMPOSICOES": 0,
        "COMPOSICOES_EXCLUIDAS": "",
        "NM_PARCELAINICIO_DESPM": "",
        "NM_PARCELAFIM_DESPM": "",
        "ID_DESPESA_DESPM": "",
        "CODIGOBARRAS_ANTERIOR": info_despesa["st_codigobarras_mov"],
        "PERMITE_ALTERAR_COM_COMPOSICAO": 0,
        "ATUALIZAR_FUTURAS": 0,
        "ST_CODIGOBARRAS_MOV": codigo_barras,
        "VALOR_BOLETO": "",
        "VL_PAGAMENTO": f"-{info_despesa['vl_valor']}",
        "COMPOSICOES[0][FL_IMOVEL_VAGO]": "",
        "COMPOSICOES[0][ID_IMOVEL_IMO]": comp["id_imovel_imo"],
        "COMPOSICOES[0][ID_DESPESA_DESP]": comp["id_despesa_desp"],
        "COMPOSICOES[0][ID_LANCAMENTO]": comp["id_lancamento"],
        "COMPOSICOES[0][ID_LANCAMENTO_IMODM] ": "",
        "COMPOSICOES[0][ID_FORMAPAGAMENTO]": comp["id_formapagamento"],
        "COMPOSICOES[0][ID_CONTRATO_CON]": comp["id_contrato_con"],
        "COMPOSICOES[0][NM_PROPRIETARIOS]": "",
        "COMPOSICOES[0][FL_CONCILIADO]": "",
        "COMPOSICOES[0][FL_CONTRATOATIVO]": "",
        "COMPOSICOES[0][DT_REFERENCIA]": data_venc_formatada,
        "COMPOSICOES[0][DT_VENCIMENTO]": data_venc_formatada,
        "COMPOSICOES[0][DT_COMPETENCIA]": comp["dt_competencia"],
        "COMPOSICOES[0][ID_CONTABANCO_CB]": comp["id_contabanco_cb"],
        "COMPOSICOES[0][NM_DIAVENCIMENTO]": data_venc_formatada.split("/")[1],
        "COMPOSICOES[0][DT_INICIO]": "",
        "COMPOSICOES[0][DT_FIM]": "",
        "COMPOSICOES[0][ID_CREDITO]": comp["id_credito"],
        "COMPOSICOES[0][ID_TERCEIRO_FAV]": comp["id_terceiro_fav"],
        "COMPOSICOES[0][ID_DESPESAREEMBOLSO]": "",
        "COMPOSICOES[0][ID_TEMPORARIO]": "",
        "COMPOSICOES[0][NOME_PROPRIETARIODEBITO]": "",
        "COMPOSICOES[0][FL_PERIODODESPESAPRINCIPAL]": 1,
        "COMPOSICOES[0][FL_TIPOCOMPETENCIA]": "",
        "COMPOSICOES[0][VL_PAGTOINDEVIDO]": "",
        "COMPOSICOES[0][FL_DESPESAPROPORCIONAL]": 0,
        "COMPOSICOES[0][FL_PARCELADA]": comp["fl_parcelada"],
        "COMPOSICOES[0][ID_PRODUTO_PRD]": comp["id_produto_prd"],
        "COMPOSICOES[0][ST_DESCRICAO_PRD]": comp["st_descricao_prd"],
        "COMPOSICOES[0][ST_COMPLEMENTO]": comp["st_complemento"],
        "COMPOSICOES[0][VL_VALOR]": comp["vl_valor"],
        "COMPOSICOES[0][ID_DEBITO]": comp["id_debito"],
        "COMPOSICOES[0][FL_COBRARTXADM]": comp["fl_cobrartxadm"],
        "COMPOSICOES[0][FL_CALCULARPROPORCIONALRESCISAO]": comp["fl_calcularproporcionalrescisao"],
        "COMPOSICOES[0][ID_PROPRIETARIODEBITO]": comp["id_proprietariodebito"],
        "COMPOSICOES[0][FL_DIFERENCA]": 0,
        "COMPOSICOES[0][VL_VALORORIGINAL]": comp["vl_valororiginal"],
        "COMPOSICOES[0][ID_RECEBIMENTO_RECB]": comp["id_recebimento_recb"],
        "COMPOSICOES[0][ID_REPASSE]": comp["id_repasse"],
        "COMPOSICOES[0][TEM_REPASSE_CC]": comp["tem_repasse_cc"],
        "COMPOSICOES[0][FL_ALTEROUVALOR]": comp["fl_alterouvalor"],
        "COMPOSICOES[0][NOVA_COMPOSICAO]": 0,
        "COMPOSICOES[0][FL_ALTERAR_COMPOSICOES]": 1,
        "ID_CONTABANCO_CB": info_despesa["id_contabanco_cb"],
        "INDICE_PRINCIPAL": 0,
        "ID_DEBITO": comp["id_debito"],
        "DT_ATUAL_VENCIMENTO": data_vencimento,
        "DT_REFERENCIACAIXA": data_venc_formatada,
        "FORCAR_ALTERAR": 1,
        "IDS_DELETAR": "",
        "salvar": "Alterar"
    }

    response = requests.put(url=url_put, headers=headers, data=PAYLOAD)

    if response.status_code != 200:
        log.error(f"Erro na requisição: {response.status_code}")
        renomear_e_mover_arquivo(
            pdf, "Erro na requisição post", caminho_iptu_erro)
        raise requests.exceptions.HTTPError

    if "COM ERRO" in response.text:
        log.error(f"Erro no processamento do ítem.\n{response.text}")
        renomear_e_mover_arquivo(
            pdf, "Erro interno da requisição", caminho_iptu_erro)
        raise requests.exceptions.HTTPError

    return


def lancar_valor_despesa_api_sl(url_put: str, headers: dict, info_despesa: dict, codigo_barras: str, data_venc_formatada: str) -> None:

    comp = info_despesa["composicoes"][0]

    PAYLOAD = {
        "ID_LANCTOPROGREALIZADO_LPR": "",
        "ID_LANCAMENTO_IMOD": "",
        "ID_LANCAMENTO_IMODM": "",
        "DT_VENCIMENTO": data_venc_formatada,
        "DT_LIQUIDACAO_MOV": "",
        "VL_VALOR_IMOD": f"-{info_despesa['vl_total']}",
        "NM_NUMERO_CH": "0",
        "ID_DEBITO_IMOD": "",
        "ID_RECEBIMENTO_RECB": "",
        "ID_REPASSE_REP": "",
        "ID_FORMAPAGAMENTO_IMOD": info_despesa["id_formapagamento"],
        "FL_MANTERCHAVE": "1",
        "NM_TAGLIQUIDACAO": "",
        "NM_TAGCRIACAO": "",
        "DT_ATUAL_COMPETENCIA": data_inicial,
        "FL_DIFERENCA": "0",
        "ID_TERCEIRO_FAV": info_despesa["id_terceiro_fav"],
        "FL_TIPODESPESA": "4",
        "VL_TOTAL": info_despesa["vl_total"],
        "ID_LANCAMENTO": info_despesa["id_lancamento"],
        "ID_PRODUTO_PRD": info_despesa["id_produto_prd"],
        "FL_STATUS_MOV": "1",
        "ID_CREDITO": info_despesa["id_credito"],
        "DT_REFERENCIA": data_venc_formatada,
        "ID_CONTRATO_CON": info_despesa["id_contrato_con"],
        "ID_FORMAPAGAMENTO": info_despesa["id_formapagamento"],
        "ID_IMOVEL_IMO": info_despesa["id_imovel_imo"],
        "DT_COMPETENCIA": data_inicial,
        "ID_CONTABANCO_MOV": "",
        "FL_CONCILIADO": "",
        "FL_ALTERAR_COMPOSICOES": "0",
        "COMPOSICOES_EXCLUIDAS": "",
        "NM_PARCELAINICIO_DESPM": info_despesa["nm_parcelainicio_despm"],
        "NM_PARCELAFIM_DESPM": info_despesa["nm_parcelafim_despm"],
        "ID_DESPESA_DESPM": "",
        "CODIGOBARRAS_ANTERIOR": "",
        "PERMITE_ALTERAR_COM_COMPOSICAO": "0",
        "ATUALIZAR_FUTURAS": "0",
        "ST_CODIGOBARRAS_MOV": codigo_barras,
        "VALOR_BOLETO": "",
        "VL_PAGAMENTO": f"-{info_despesa['vl_total']}",

        # Bloco da composição (índice 0)
        "COMPOSICOES[0][FL_IMOVEL_VAGO]": "",
        "COMPOSICOES[0][ID_IMOVEL_IMO]": comp["id_imovel_imo"],
        "COMPOSICOES[0][ID_DESPESA_DESP]": "",
        "COMPOSICOES[0][ID_LANCAMENTO]": comp["id_lancamento"],
        "COMPOSICOES[0][ID_LANCAMENTO_IMODM]": comp["id_lancamento"],
        "COMPOSICOES[0][ID_FORMAPAGAMENTO]": comp["id_formapagamento"],
        "COMPOSICOES[0][ID_CONTRATO_CON]": comp["id_contrato_con"],
        "COMPOSICOES[0][NM_PROPRIETARIOS]": "",
        "COMPOSICOES[0][FL_CONCILIADO]": "",
        "COMPOSICOES[0][FL_CONTRATOATIVO]": "",
        "COMPOSICOES[0][DT_REFERENCIA]": data_venc_formatada,
        "COMPOSICOES[0][DT_VENCIMENTO]": data_venc_formatada,
        "COMPOSICOES[0][DT_COMPETENCIA]": data_inicial,
        "COMPOSICOES[0][ID_CONTABANCO_CB]": comp["id_contabanco_cb"],
        "COMPOSICOES[0][NM_DIAVENCIMENTO]": data_venc_formatada.split("/")[1],
        "COMPOSICOES[0][DT_INICIO]": comp["dt_inicio"],
        "COMPOSICOES[0][DT_FIM]": comp["dt_fim"],
        "COMPOSICOES[0][ID_CREDITO]": comp["id_credito"],
        "COMPOSICOES[0][ID_TERCEIRO_FAV]": comp["id_terceiro_fav"],
        "COMPOSICOES[0][ID_DESPESAREEMBOLSO]": "",
        "COMPOSICOES[0][ID_TEMPORARIO]": "",
        "COMPOSICOES[0][NOME_PROPRIETARIODEBITO]": "",
        "COMPOSICOES[0][FL_PERIODODESPESAPRINCIPAL]": "1",
        "COMPOSICOES[0][FL_TIPOCOMPETENCIA]": "",
        "COMPOSICOES[0][VL_PAGTOINDEVIDO]": "",
        "COMPOSICOES[0][FL_DESPESAPROPORCIONAL]": "",
        "COMPOSICOES[0][FL_PARCELADA]": "",
        "COMPOSICOES[0][ID_PRODUTO_PRD]": comp["id_produto_prd"],
        "COMPOSICOES[0][ST_DESCRICAO_PRD]": comp["st_descricao_prd"],
        "COMPOSICOES[0][ST_COMPLEMENTO]": "",
        "COMPOSICOES[0][VL_VALOR]": comp["vl_valor"],
        "COMPOSICOES[0][ID_DEBITO]": comp["id_debito"],
        "COMPOSICOES[0][FL_COBRARTXADM]": comp["fl_cobrartxadm"],
        "COMPOSICOES[0][ID_PROPRIETARIODEBITO]": "",
        "COMPOSICOES[0][FL_DIFERENCA]": "0",
        "COMPOSICOES[0][VL_VALORORIGINAL]": "",
        "COMPOSICOES[0][ID_RECEBIMENTO_RECB]": "",
        "COMPOSICOES[0][ID_REPASSE]": "",
        "COMPOSICOES[0][TEM_REPASSE_CC]": "",
        "COMPOSICOES[0][FL_ALTEROUVALOR]": "",
        "COMPOSICOES[0][NOVA_COMPOSICAO]": "0",
        "COMPOSICOES[0][FL_ALTERAR_COMPOSICOES]": "1",

        # Campos finais
        "ID_CONTABANCO_CB": info_despesa["id_contabanco_cb"],
        "ID_DEBITO": comp["id_debito"],
        "FL_TIPOCOMPETENCIA": info_despesa["fl_tipocompetencia"],
        "ID_DESPESA": id_despesa_despm,
        "salvar": "Lançar",
        "DT_REFERENCIACAIXA": data_venc_formatada
    }

    response = requests.put(url=url_put, headers=headers, data=PAYLOAD)

    if response.status_code != 200:
        log.error(f"Erro na requisição: {response.status_code}")
        renomear_e_mover_arquivo(
            pdf, "Erro na requisição post", caminho_iptu_erro)
        raise requests.exceptions.HTTPError

    if "COM ERRO" in response.text:
        log.error(f"Erro no processamento do ítem.\n{response.text}")
        renomear_e_mover_arquivo(
            pdf, "Erro interno da requisição", caminho_iptu_erro)
        raise requests.exceptions.HTTPError

    return


def renomear_e_mover_arquivo(path_arquivo: Path, info: str | list[str], novo_diretorio: Path) -> None:
    """Renomeia o arquivo com a mensagem de resultado e move o arquivo para outra pasta."""

    novo_diretorio = Path(novo_diretorio)

    # Garante que o diretório de destino existe
    novo_diretorio.mkdir(parents=True, exist_ok=True)

    # Monta o nome base
    base_nome = f"{path_arquivo.stem} - {info}"
    extensao = path_arquivo.suffix

    # Caminho inicial
    novo_caminho = novo_diretorio / f"{base_nome}{extensao}"

    # Se já existir, adiciona um contador
    contador = 1
    while novo_caminho.exists():
        novo_caminho = novo_diretorio / f"{base_nome} ({contador}){extensao}"
        contador += 1

    # Move + renomeia
    shutil.move(path_arquivo, novo_caminho)

    # Loga o resultado
    log.info(f"Arquivo movido para: {str(novo_diretorio)}")


if __name__ == "__main__":

    log.info("========= APLICAÇÃO INICIADA. =================================")

    config = init_config()

    try:
        caminho_busca_iptu = config["[PATHS]"]["iptu_a_lancar"]
        caminho_iptu_ok = config["[PATHS]"]["iptu_ok"]
        caminho_iptu_erro = config["[PATHS]"]["iptu_erro"]

        URL_GET = config["[API]"]["url_get"]
        URL_ALTERAR_DESP = config["[API]"]["url_put_alterar_despesa"]
        URL_LANCAR_DESP = config["[API]"]["url_put_lancar_despesa"]
        URL_INFO_DESP = config["[API]"]["url_post_info_despesa"]

        HEADERS = config["[API]"]["headers"]

        TEMP_HEADERS = deepcopy(HEADERS)
        del TEMP_HEADERS["Content-Type"]
    except KeyError:
        log.error("Chave não encontrada no arquivo de configuração.")
        raise

    MES_LANCAMENTO, ANO_LANCAMENTO = obter_competencia_atual()

    data_inicial = f"{MES_LANCAMENTO}/1/{ANO_LANCAMENTO}"
    data_final = f"{MES_LANCAMENTO}/30/{ANO_LANCAMENTO}"

    lista_contratos = get_contratos_api("contratos", URL_GET, HEADERS)
    dict_id_contratos = relacionar_codigo_e_id_contratos(lista_contratos)

    lista_pdfs = listar_arquivos_pdf(caminho_busca_iptu)

    for pdf in lista_pdfs:

        cod_contrato = pdf.stem.upper()
        log.info(f"[{cod_contrato}] IMÓVEL ATUAL")

        try:
            id_contrato = dict_id_contratos[cod_contrato].upper()
        except (ValueError, KeyError):
            log.error(
                f"[{cod_contrato}] Id do contrato não encontrado na relação.")
            renomear_e_mover_arquivo(
                pdf, "Id contrato não encontrado", caminho_iptu_erro)
            continue

        data_vencimento, cod_barras, valor_total = extrair_dados_pdf(
            pdf, MES_LANCAMENTO)
        data_venc_formatada = formatar_data_vencimento(data_vencimento)

        log.info(f"Id do contrato: {id_contrato}")
        log.info(f"Data vencimento: {data_vencimento}")
        log.info(f"Código de Barras: {cod_barras}")
        log.info(f"Valor Total: {valor_total}")

        # Valores para testes: ===========================================
        # id_contrato = "11"
        # valor_total = "94.20"
        # cod_barras = "816200000007942036592023510073102509900001709014"
        # data_vencimento = "05/09/2025"
        # data_venc_formatada = "09/05/2025"
        # ================================================================

        payload_get_despesas = {
            "itensPorPagina": 150,
            "pagina": 1,
            "dtInicioMensal": data_inicial,
            "dtFimMensal": data_final,
            "idContrato": id_contrato,
            "idProduto": 6,  # IPTU
        }
        try:
            despesas_contrato = get_despesas_iptu_api(
                "despesas", URL_GET, HEADERS, payload_get_despesas)
        except ValueError:
            log.error("Não foram encontradas despesas IPTU para o contrato")
            renomear_e_mover_arquivo(
                pdf, "Sem despesas IPTU para o contrato", caminho_iptu_erro)
            continue

        id_lancamento = None
        mensagem = []
        for despesa in despesas_contrato:

            descricao_prod = despesa["st_descricao_prd"]
            valor_lancamento = despesa["vl_valor_imod"]
            debito = despesa["id_debito_imod"]
            id_despesa = despesa["id_despesa_desp"]
            id_despesa_despm = despesa["id_despesa_despm"]

            if descricao_prod == "IPTU" and valor_lancamento == valor_total:

                if debito != "2":
                    # Se a despesa tem lançamento válido, mas não está para o Locatário
                    if id_despesa or id_despesa_despm:
                        mensagem.append("Débito não está para o locatário")
                        break
                else:
                    continue

                if id_despesa:
                    id_lancamento = id_despesa
                    tipo_form = "FormAlterarValorDespesaPrincipal"
                    break
                elif id_despesa_despm:
                    id_lancamento = id_despesa_despm
                    tipo_form = "FormLancarDespesaPrincipal"
                    break
                else:
                    mensagem.append("Sem id lançamento")
                    continue
            else:
                mensagem.append("Sem lançamento")
                continue

        if id_lancamento:
            # ALTERAR VALOR NO A PAGAR
            if id_despesa:
                payload_info_despesa = {
                    "itensPorPagina": 150,
                    "pagina": 1,
                    "ID_DESPESA_DESP": id_despesa,
                    "ID_DESPESA_DESPM": id_despesa_despm,
                    "DT_FIM": data_final,
                    "FORM": tipo_form
                }
                info_despesa = get_info_despesa(
                    URL_INFO_DESP, HEADERS, payload_info_despesa)

                try:
                    alterar_valor_despesa_api_sl(
                        URL_ALTERAR_DESP, TEMP_HEADERS, info_despesa, cod_barras, data_venc_formatada)

                    log.info(f"[{cod_contrato}] Alterado com sucesso.")
                    renomear_e_mover_arquivo(pdf, "OK", caminho_iptu_ok)

                except requests.exceptions.HTTPError:
                    continue

                except Exception as e:
                    log.error(f"Erro PUT request: {e}")
                    renomear_e_mover_arquivo(
                        pdf, "Erro PUT request", caminho_iptu_erro)
                    continue

            # LANÇAR DESPESA
            elif id_despesa_despm:
                payload_info_despesa = {
                    "itensPorPagina": 150,
                    "pagina": 1,
                    "ID_DESPESA_DESP": id_despesa,
                    "ID_DESPESA_DESPM": id_despesa_despm,
                    "DT_FIM": data_final,
                    "DT_INICIO": data_inicial,
                    "FORM": tipo_form
                }
                info_despesa = get_info_despesa(
                    URL_INFO_DESP, HEADERS, payload_info_despesa)

                try:
                    lancar_valor_despesa_api_sl(
                        URL_LANCAR_DESP, TEMP_HEADERS, info_despesa, cod_barras, data_venc_formatada)

                    log.info(f"[{cod_contrato}] Lançado com sucesso.")
                    renomear_e_mover_arquivo(pdf, "OK", caminho_iptu_ok)

                except requests.exceptions.HTTPError:
                    continue

                except Exception as e:
                    log.error(f"Erro PUT request: {e}")
                    renomear_e_mover_arquivo(
                        pdf, "Erro PUT request", caminho_iptu_erro)
                    continue
        else:
            log.error(f"[{cod_contrato}] {mensagem}")
            renomear_e_mover_arquivo(pdf, mensagem, caminho_iptu_erro)
