import tkinter as tk
from tkinter import ttk, messagebox
import json
import os
import requests
import pandas as pd
import threading
import time

CONFIG_FILE = "config.json"

class DashboardApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Download de Dados OS")
        self.geometry("1000x600")
        
        self.config_data = self.load_config()
        
        self.create_widgets()
        
    def load_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Erro ao ler config.json: {e}")
        return {'login': '', 'password': ''}

    def create_widgets(self):
        # Top frame for button and status
        top_frame = tk.Frame(self)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=10, pady=10)
        
        self.btn_download = ttk.Button(top_frame, text="Atualizar e Baixar Planilha", command=self.start_download_thread)
        self.btn_download.pack(side=tk.LEFT)
        
        self.lbl_status = tk.Label(top_frame, text="Status: Aguardando...", fg="blue")
        self.lbl_status.pack(side=tk.LEFT, padx=20)
        
        # Notebook for tabs
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(expand=True, fill=tk.BOTH, padx=10, pady=10)
        
        # Tabs
        self.tab_historico = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_historico, text="Histórico OS")
        
        self.tab_detalhes = ttk.Frame(self.notebook)
        self.notebook.add(self.tab_detalhes, text="Detalhes OS")
        
        # Treeviews configuration
        self.tree_historico = ttk.Treeview(self.tab_historico)
        self.tree_historico.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
        
        scrollbar_v_h = ttk.Scrollbar(self.tab_historico, orient=tk.VERTICAL, command=self.tree_historico.yview)
        scrollbar_h_h = ttk.Scrollbar(self.tab_historico, orient=tk.HORIZONTAL, command=self.tree_historico.xview)
        self.tree_historico.configure(yscrollcommand=scrollbar_v_h.set, xscrollcommand=scrollbar_h_h.set)
        scrollbar_v_h.pack(side=tk.RIGHT, fill=tk.Y)
        scrollbar_h_h.pack(side=tk.BOTTOM, fill=tk.X)
        
        self.tree_detalhes = ttk.Treeview(self.tab_detalhes)
        self.tree_detalhes.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)

        scrollbar_v_d = ttk.Scrollbar(self.tab_detalhes, orient=tk.VERTICAL, command=self.tree_detalhes.yview)
        scrollbar_h_d = ttk.Scrollbar(self.tab_detalhes, orient=tk.HORIZONTAL, command=self.tree_detalhes.xview)
        self.tree_detalhes.configure(yscrollcommand=scrollbar_v_d.set, xscrollcommand=scrollbar_h_d.set)
        scrollbar_v_d.pack(side=tk.RIGHT, fill=tk.Y)
        scrollbar_h_d.pack(side=tk.BOTTOM, fill=tk.X)

    def update_status(self, message):
        self.lbl_status.config(text=f"Status: {message}")
        self.update_idletasks()

    def _get_token(self):
        login = self.config_data.get('login')
        password = self.config_data.get('password')
        if not login or not password:
            messagebox.showerror("Erro", "Login e senha não configurados no config.json")
            return None
            
        try:
            auth_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/auth/V1"
            auth_payload = {"login": login, "password": password}
            auth_response = requests.post(auth_url, json=auth_payload, timeout=10)
            auth_response.raise_for_status()
            auth_data = auth_response.json()
            return auth_data.get("token")
        except Exception as e:
            messagebox.showerror("Erro de Autenticação", f"Não foi possível autenticar:\n{e}")
            return None

    def start_download_thread(self):
        self.btn_download.config(state=tk.DISABLED)
        thread = threading.Thread(target=self.fetch_and_download)
        thread.daemon = True
        thread.start()

    def fetch_and_download(self):
        self.update_status("Obtendo token...")
        token = self._get_token()
        if not token:
            self.after(0, lambda: self.btn_download.config(state=tk.NORMAL))
            self.update_status("Falha na autenticação.")
            return

        headers = {"Authorization": token}
        
        try:
            # Histórico
            self.update_status("Carregando histórico...")
            data_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/last-update/2020-01-01"
            response = requests.get(data_url, headers=headers, timeout=60)
            response.raise_for_status()
            historico_data = response.json().get("data", [])
            
            df_historico = pd.DataFrame(historico_data)
            
            # Detalhes
            self.update_status("Carregando detalhes...")
            all_details = []
            total = len(historico_data)
            for i, os_item in enumerate(historico_data):
                numeroos = os_item.get("numeroos")
                if numeroos:
                    if (i + 1) % 20 == 0:
                        self.update_status(f"Carregando detalhes... {i+1}/{total}")
                    
                    details_url = f"https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/os-details/{numeroos}"
                    try:
                        det_resp = requests.get(details_url, headers=headers, timeout=15)
                        if det_resp.status_code == 200 and det_resp.json().get("status"):
                            data_arr = det_resp.json().get("data")
                            if data_arr and data_arr[0] is not None:
                                all_details.extend(data_arr)
                    except requests.exceptions.RequestException:
                        pass # Continua se der erro em uma OS especifica
                    
                    time.sleep(0.05) # Para nao sobrecarregar a API
            
            df_detalhes = pd.DataFrame(all_details)
            
            self.update_status("Exportando para Excel...")
            # Save to Excel
            excel_path = os.path.join(os.getcwd(), "planilha_os_exportada.xlsx")
            with pd.ExcelWriter(excel_path) as writer:
                df_historico.to_excel(writer, sheet_name='Historico_OS', index=False)
                if not df_detalhes.empty:
                    df_detalhes.to_excel(writer, sheet_name='Detalhes_OS', index=False)
            
            # Update GUI Treeviews
            self.update_status("Atualizando interface com os dados...")
            self.after(0, self.populate_treeview, self.tree_historico, df_historico)
            self.after(0, self.populate_treeview, self.tree_detalhes, df_detalhes)
            
            self.update_status(f"Concluído! Arquivo salvo como planilha_os_exportada.xlsx")
            messagebox.showinfo("Sucesso", f"Download concluído e planilha gerada com sucesso em:\n{excel_path}")
            
        except Exception as e:
            self.update_status(f"Erro: {str(e)}")
            messagebox.showerror("Erro na Operação", f"Ocorreu um erro ao buscar os dados:\n{str(e)}")
        finally:
            self.after(0, lambda: self.btn_download.config(state=tk.NORMAL))

    def populate_treeview(self, tree, df):
        # Clear existing
        for item in tree.get_children():
            tree.delete(item)
            
        if df.empty:
            return
            
        # Set columns
        cols = list(df.columns)
        tree["columns"] = cols
        tree["show"] = "headings"
        
        for col in cols:
            tree.heading(col, text=col)
            tree.column(col, width=120, anchor=tk.W)
            
        # Add rows (convert all data to string to avoid tk errors with some types)
        for _, row in df.iterrows():
            values = [str(val) for val in row]
            tree.insert("", "end", values=values)

if __name__ == "__main__":
    app = DashboardApp()
    app.mainloop()
