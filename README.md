# Dashboard Manutenção

## Banco de dados (PostgreSQL / Supabase)

O projeto usa **PostgreSQL** (ex.: Supabase). A connection string é lida da variável de ambiente **`DATABASE_URL`**.

### Deploy no Streamlit Cloud

1. No [Streamlit Cloud](https://share.streamlit.io), ao criar ou editar o app, abra **Settings** → **Secrets** (ou **Environment variables**).
2. Adicione a variável:
   - **Nome:** `DATABASE_URL`
   - **Valor:** sua connection string completa, por exemplo:
     ```
     postgresql://postgres:SUA_SENHA@db.rvxnkscptvpxobzvvole.supabase.co:5432/postgres
     ```
3. Substitua `SUA_SENHA` pela senha real do banco (Supabase: Project Settings → Database → Connection string → senha).

Se `DATABASE_URL` não estiver definida, o código usa um fallback com `[YOUR-PASSWORD]` (só para desenvolvimento; no Cloud é obrigatório definir a variável).
