#!/usr/bin/env bash

set -euo pipefail

readonly START_TIME="$SECONDS"
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly ENV_DIR="$SCRIPT_DIR/env"
readonly VALID_ENVIRONMENTS=(
  "dev"
  "hml"
  "prd"
)
readonly REMOTE_REQUIRED_ENV_VARS=(
  "REMOTE_SSH_HOST"
  "REMOTE_SSH_USER"
  "REMOTE_SSH_KEY_PATH"
  "REMOTE_EXPORT_SCRIPT"
)
readonly LOCAL_REQUIRED_ENV_VARS=(
  "LOCAL_PGHOST"
  "LOCAL_PGPORT"
  "LOCAL_PGDATABASE"
  "LOCAL_PGUSER"
  "LOCAL_PGPASSWORD"
)
readonly REMOTE_CONNECTION_ERROR_MESSAGE="Verifique se chave pem está correta e se o acesso remoto foi solicitado no Portal do Azure"

SQL_FILE=""
SSH_LOG_FILE=""
SCP_LOG_FILE=""
TEMP_DATABASE=""
LOCAL_DUMP_FILE=""
REMOTE_DUMP_FILE=""
CLEANUP_TEMP_DATABASE=0
CLEANUP_REMOTE_DUMP=0

usage() {
  cat <<'EOF'
Uso:
  ./importar-schema-remoto.sh <ambiente> [schema-remoto] [schema-local]

Parâmetros:
  ambiente       Ambiente remoto: dev, hml ou prd
  schema-remoto  Schema a ser exportado do servidor remoto (opcional, padrão: public)
  schema-local   Nome do schema no banco local (opcional, padrão: mesmo schema-remoto)

Configuração remota por ambiente:
  Arquivo esperado em:
    postgres/copia-schema/env/<ambiente>.env

Variáveis remotas obrigatórias no arquivo:
  REMOTE_SSH_HOST
  REMOTE_SSH_USER
  REMOTE_SSH_KEY_PATH
  REMOTE_EXPORT_SCRIPT

Variáveis remotas opcionais:
  REMOTE_SSH_PORT (padrão: 22)
  DEFAULT_SCHEMA (padrão: public)

Variáveis de ambiente do PostgreSQL local obrigatórias:
  LOCAL_PGHOST
  LOCAL_PGPORT
  LOCAL_PGDATABASE
  LOCAL_PGUSER
  LOCAL_PGPASSWORD
EOF
}

fail() {
  echo "Erro: $*" >&2
  exit 1
}

log_step() {
  echo
  echo "==> $*"
}

log_info() {
  echo "  - $*"
}

is_valid_environment() {
  local environment="$1"
  local valid_environment

  for valid_environment in "${VALID_ENVIRONMENTS[@]}"; do
    if [[ "$valid_environment" == "$environment" ]]; then
      return 0
    fi
  done

  return 1
}

load_environment_file() {
  local environment="$1"
  local env_file="$ENV_DIR/${environment}.env"

  [[ -f "$env_file" ]] || fail "arquivo de configuração do ambiente não encontrado em '$env_file'."

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
}

escape_sql_literal() {
  printf "%s" "$1" | sed "s/'/''/g"
}

escape_sql_identifier() {
  printf "%s" "$1" | sed 's/"/""/g'
}

require_command() {
  local command_name="$1"

  command -v "$command_name" >/dev/null 2>&1 || \
    fail "comando '$command_name' não encontrado."
}

format_duration() {
  local total_seconds="$1"
  local hours=$((total_seconds / 3600))
  local minutes=$(((total_seconds % 3600) / 60))
  local seconds=$((total_seconds % 60))

  printf '%02dh %02dm %02ds' "$hours" "$minutes" "$seconds"
}

print_execution_duration() {
  local duration=$((SECONDS - START_TIME))
  echo "Duracao da execucao: $(format_duration "$duration")."
}

validate_env_vars() {
  local group_name="$1"
  shift

  local missing_vars=()
  local env_var

  for env_var in "$@"; do
    if [[ -z "${!env_var:-}" ]]; then
      missing_vars+=("$env_var")
    fi
  done

  if (( ${#missing_vars[@]} > 0 )); then
    printf 'As seguintes variáveis de ambiente %s são obrigatórias e não foram definidas:\n' "$group_name" >&2
    printf '  - %s\n' "${missing_vars[@]}" >&2
    usage >&2
    exit 1
  fi
}

run_remote_command() {
  local log_file="$1"
  shift

  if ssh "$@" >"$log_file" 2>&1; then
    return 0
  else
    local exit_code=$?
    if [[ "$exit_code" -eq 255 ]]; then
      echo "$REMOTE_CONNECTION_ERROR_MESSAGE" >&2
    else
      cat "$log_file" >&2
    fi
    exit "$exit_code"
  fi
}

copy_remote_file() {
  local log_file="$1"
  shift

  if scp "$@" >"$log_file" 2>&1; then
    return 0
  else
    local exit_code=$?
    if [[ "$exit_code" -eq 255 ]]; then
      echo "$REMOTE_CONNECTION_ERROR_MESSAGE" >&2
    else
      cat "$log_file" >&2
    fi
    exit "$exit_code"
  fi
}

cleanup_remote_dump() {
  local remote_port="${REMOTE_SSH_PORT:-22}"

  [[ "${CLEANUP_REMOTE_DUMP:-0}" -eq 1 ]] || return 0
  [[ -n "${REMOTE_DUMP_FILE:-}" ]] || return 0
  [[ -n "${REMOTE_SSH_HOST:-}" ]] || return 0
  [[ -n "${REMOTE_SSH_USER:-}" ]] || return 0
  [[ -n "${REMOTE_SSH_KEY_PATH:-}" ]] || return 0

  if ssh \
    -i "$REMOTE_SSH_KEY_PATH" \
    -p "$remote_port" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    "${REMOTE_SSH_USER}@${REMOTE_SSH_HOST}" \
    rm -f "$REMOTE_DUMP_FILE" >/dev/null 2>&1; then
    echo "  - Dump remoto removido: '$REMOTE_DUMP_FILE'."
  else
    echo "Aviso: falha ao remover dump remoto '$REMOTE_DUMP_FILE'." >&2
  fi
}

cleanup() {
  local exit_code=$?

  cleanup_remote_dump
  rm -f "${SQL_FILE:-}" "${SSH_LOG_FILE:-}" "${SCP_LOG_FILE:-}" "${LOCAL_DUMP_FILE:-}"
  if [[ "${CLEANUP_TEMP_DATABASE:-0}" -eq 1 ]]; then
    PGPASSWORD="$LOCAL_PGPASSWORD" dropdb \
      --if-exists \
      --host="$LOCAL_PGHOST" \
      --port="$LOCAL_PGPORT" \
      --username="$LOCAL_PGUSER" \
      --maintenance-db="$LOCAL_PGDATABASE" \
      "$TEMP_DATABASE" >/dev/null 2>&1 || true
  fi

  print_execution_duration
  exit "$exit_code"
}

main() {
  local environment="${1:-}"
  local source_schema
  local target_schema
  local remote_port="${REMOTE_SSH_PORT:-22}"
  local timestamp
  local remote_dump_file
  local local_dump_file
  local target_schema_exists
  local source_schema_literal
  local source_schema_identifier
  local target_schema_literal
  local target_schema_identifier

  [[ -n "$environment" ]] || {
    usage >&2
    fail "o ambiente deve ser informado."
  }

  is_valid_environment "$environment" || fail "ambiente inválido '$environment'. Use: dev, hml ou prd."

  load_environment_file "$environment"

  source_schema="${2:-${DEFAULT_SCHEMA:-public}}"
  target_schema="${3:-$source_schema}"
  remote_port="${REMOTE_SSH_PORT:-22}"

  require_command "ssh"
  require_command "scp"
  require_command "pg_restore"
  require_command "psql"
  require_command "pg_dump"
  require_command "createdb"
  require_command "dropdb"

  validate_env_vars "de SSH" "${REMOTE_REQUIRED_ENV_VARS[@]}"
  validate_env_vars "do PostgreSQL local" "${LOCAL_REQUIRED_ENV_VARS[@]}"

  [[ -f "$REMOTE_SSH_KEY_PATH" ]] || fail "arquivo da chave pem não encontrado em '$REMOTE_SSH_KEY_PATH'."

  timestamp="$(date +%Y%m%d%H%M%S)"
  remote_dump_file="/tmp/schema-${timestamp}.dump"
  local_dump_file="./$(basename "$remote_dump_file")"
  REMOTE_DUMP_FILE="$remote_dump_file"
  LOCAL_DUMP_FILE="$local_dump_file"
  SQL_FILE="$(mktemp)"
  SSH_LOG_FILE="$(mktemp)"
  SCP_LOG_FILE="$(mktemp)"
  TEMP_DATABASE="tmp_import_schema_${timestamp}"
  source_schema_literal="$(escape_sql_literal "$source_schema")"
  source_schema_identifier="$(escape_sql_identifier "$source_schema")"
  target_schema_literal="$(escape_sql_literal "$target_schema")"
  target_schema_identifier="$(escape_sql_identifier "$target_schema")"

  trap cleanup EXIT

  log_step "Iniciando importacao de schema remoto"
  log_info "Ambiente remoto: '$environment' (${REMOTE_SSH_USER}@${REMOTE_SSH_HOST})."
  log_info "Schema remoto: '$source_schema'."
  log_info "Schema local: '$target_schema'."
  log_info "Dump remoto temporario: '$REMOTE_DUMP_FILE'."
  log_info "Dump local temporario: '$LOCAL_DUMP_FILE'."

  log_step "Gerando dump no servidor remoto"
  run_remote_command "$SSH_LOG_FILE" \
    -i "$REMOTE_SSH_KEY_PATH" \
    -p "$remote_port" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    "${REMOTE_SSH_USER}@${REMOTE_SSH_HOST}" \
    "$REMOTE_EXPORT_SCRIPT" "$remote_dump_file" "$source_schema"
  CLEANUP_REMOTE_DUMP=1
  log_info "Dump remoto gerado com sucesso."

  log_step "Copiando dump remoto para a maquina local"
  copy_remote_file "$SCP_LOG_FILE" \
    -i "$REMOTE_SSH_KEY_PATH" \
    -P "$remote_port" \
    -o BatchMode=yes \
    -o ConnectTimeout=10 \
    "${REMOTE_SSH_USER}@${REMOTE_SSH_HOST}:$remote_dump_file" \
    "$local_dump_file"
  log_info "Dump copiado para '$LOCAL_DUMP_FILE'."

  log_step "Validando schema de destino no banco local"
  target_schema_exists="$(
    PGPASSWORD="$LOCAL_PGPASSWORD" psql \
      --host="$LOCAL_PGHOST" \
      --port="$LOCAL_PGPORT" \
      --username="$LOCAL_PGUSER" \
      --dbname="$LOCAL_PGDATABASE" \
      --tuples-only \
      --no-align \
      --quiet \
      --command="SELECT 1 FROM information_schema.schemata WHERE schema_name = '$target_schema_literal' LIMIT 1;"
  )"

  [[ -z "$target_schema_exists" ]] || fail "o schema '$target_schema' já existe no banco local '$LOCAL_PGDATABASE'."
  log_info "Schema de destino disponivel no banco '$LOCAL_PGDATABASE'."

  log_step "Criando banco temporario local"
  PGPASSWORD="$LOCAL_PGPASSWORD" createdb \
    --host="$LOCAL_PGHOST" \
    --port="$LOCAL_PGPORT" \
    --username="$LOCAL_PGUSER" \
    --maintenance-db="$LOCAL_PGDATABASE" \
    "$TEMP_DATABASE"
  CLEANUP_TEMP_DATABASE=1
  log_info "Banco temporario criado: '$TEMP_DATABASE'."

  if [[ "$source_schema" == "public" ]]; then
    log_step "Removendo schema public padrao do banco temporario"
    PGPASSWORD="$LOCAL_PGPASSWORD" psql \
      --host="$LOCAL_PGHOST" \
      --port="$LOCAL_PGPORT" \
      --username="$LOCAL_PGUSER" \
      --dbname="$TEMP_DATABASE" \
      --set=ON_ERROR_STOP=1 \
      --command='DROP SCHEMA public CASCADE;'
    log_info "Schema public padrao removido do banco temporario."
  fi

  log_step "Restaurando dump no banco temporario"
  pg_restore \
    --no-owner \
    --no-privileges \
    --file=- \
    "$local_dump_file" | \
    grep -v "^SET transaction_timeout" | \
    sed -E 's/(^|\t)-?Infinity(\t|$)/\1\\N\2/g' | \
    PGPASSWORD="$LOCAL_PGPASSWORD" psql \
      --host="$LOCAL_PGHOST" \
      --port="$LOCAL_PGPORT" \
      --username="$LOCAL_PGUSER" \
      --dbname="$TEMP_DATABASE" \
      --set=ON_ERROR_STOP=1 \
      --quiet
  log_info "Restore concluido no banco '$TEMP_DATABASE'."

  if [[ "$source_schema" != "$target_schema" ]]; then
    log_step "Renomeando schema no banco temporario"
    PGPASSWORD="$LOCAL_PGPASSWORD" psql \
      --host="$LOCAL_PGHOST" \
      --port="$LOCAL_PGPORT" \
      --username="$LOCAL_PGUSER" \
      --dbname="$TEMP_DATABASE" \
      --set=ON_ERROR_STOP=1 \
      --command="ALTER SCHEMA \"$source_schema_identifier\" RENAME TO \"$target_schema_identifier\";"
    log_info "Schema renomeado de '$source_schema' para '$target_schema'."
  fi

  log_step "Gerando SQL plain a partir do banco temporario"
  PGPASSWORD="$LOCAL_PGPASSWORD" pg_dump \
    --format=plain \
    --schema="$target_schema" \
    --no-owner \
    --no-privileges \
    --host="$LOCAL_PGHOST" \
    --port="$LOCAL_PGPORT" \
    --username="$LOCAL_PGUSER" \
    --dbname="$TEMP_DATABASE" \
    --file="$SQL_FILE"
  log_info "Arquivo SQL temporario gerado em '$SQL_FILE'."

  log_step "Aplicando SQL no banco local"
  sed -i '/^SET transaction_timeout/d' "$SQL_FILE"
  PGPASSWORD="$LOCAL_PGPASSWORD" psql \
    --host="$LOCAL_PGHOST" \
    --port="$LOCAL_PGPORT" \
    --username="$LOCAL_PGUSER" \
    --dbname="$LOCAL_PGDATABASE" \
    --set=ON_ERROR_STOP=1 \
    --file="$SQL_FILE"
  log_info "Schema aplicado no banco local '$LOCAL_PGDATABASE'."

  log_step "Removendo banco temporario local"
  PGPASSWORD="$LOCAL_PGPASSWORD" dropdb \
    --if-exists \
    --host="$LOCAL_PGHOST" \
    --port="$LOCAL_PGPORT" \
    --username="$LOCAL_PGUSER" \
    --maintenance-db="$LOCAL_PGDATABASE" \
    "$TEMP_DATABASE"
  CLEANUP_TEMP_DATABASE=0
  log_info "Banco temporario removido: '$TEMP_DATABASE'."

  log_step "Importacao concluida"
  log_info "Schema '$source_schema' importado para '$target_schema'."
  log_info "A remocao do dump remoto sera executada no encerramento do script."
}

main "$@"
