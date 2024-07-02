import pandas as pd

try:
    # Ler o arquivo CSV
    df = pd.read_csv('impact-report.csv', encoding='utf-8')  # ou 'ISO-8859-1' se necessário

    # Exibir as colunas disponíveis
    print(f"Colunas disponíveis: {df.columns}")

    # Remover espaços extras dos nomes das colunas
    df.columns = df.columns.str.strip()

    # Verificar a existência das colunas relevantes
    if all(col in df.columns for col in ['Event Type', 'Sub Id 1', 'Sub Id 2', 'Sub Id 3', 'Action Earnings', 'Customer_Id', 'Status']):
        print("Todas as colunas necessárias estão presentes.")
    else:
        raise ValueError("Uma ou mais colunas necessárias NÃO estão presentes. Verifique o nome das colunas.")

    # Verificar os valores únicos na coluna 'Event Type'
    print(f"Valores únicos na coluna 'Event Type': {df['Event Type'].unique()}")

    # Função para calcular faturamento e previsão de faturamento por Sub Id
    def calcular_faturamento(df, sub_id_col):
        df['Event Type'] = df['Event Type'].str.strip()  # Remover espaços extras
        
        faturamento_atual = df[df['Event Type'] == 'Online Sale API'].groupby(sub_id_col)['Action Earnings'].sum().reset_index()
        faturamento_atual.columns = [sub_id_col, 'Faturamento Atual']
        
        previsao_free_trial = df[df['Event Type'] == 'Free Trial API'].groupby(sub_id_col)['Action Earnings'].sum().reset_index()
        previsao_free_trial.columns = [sub_id_col, 'Previsão Free Trial']

        previsao_paid_trial = df[df['Event Type'] == 'Paid Trial API'].groupby(sub_id_col)['Action Earnings'].sum().reset_index()
        previsao_paid_trial.columns = [sub_id_col, 'Previsão Paid Trial']
        
        relatorio = faturamento_atual.merge(previsao_free_trial, on=sub_id_col, how='outer').merge(previsao_paid_trial, on=sub_id_col, how='outer')
        relatorio = relatorio.fillna(0)
        return relatorio

    # Função para calcular faturamento cruzando Sub Id 1 e Sub Id 2
    def calcular_faturamento_cruzado(df):
        df['Event Type'] = df['Event Type'].str.strip()  # Remover espaços extras
        
        # Filtrar apenas os registros onde Sub Id 1 é 'builder'
        df_builder = df[df['Sub Id 1'] == 'builder']
        
        faturamento_atual = df_builder[df_builder['Event Type'] == 'Online Sale API'].groupby(['Sub Id 1', 'Sub Id 2'])['Action Earnings'].sum().reset_index()
        faturamento_atual.columns = ['Sub Id 1', 'Sub Id 2', 'Faturamento Atual']
        
        previsao_free_trial = df_builder[df_builder['Event Type'] == 'Free Trial API'].groupby(['Sub Id 1', 'Sub Id 2'])['Action Earnings'].sum().reset_index()
        previsao_free_trial.columns = ['Sub Id 1', 'Sub Id 2', 'Previsão Free Trial']

        previsao_paid_trial = df_builder[df_builder['Event Type'] == 'Paid Trial API'].groupby(['Sub Id 1', 'Sub Id 2'])['Action Earnings'].sum().reset_index()
        previsao_paid_trial.columns = ['Sub Id 1', 'Sub Id 2', 'Previsão Paid Trial']
        
        relatorio_cruzado = faturamento_atual.merge(previsao_free_trial, on=['Sub Id 1', 'Sub Id 2'], how='outer').merge(previsao_paid_trial, on=['Sub Id 1', 'Sub Id 2'], how='outer')
        relatorio_cruzado = relatorio_cruzado.fillna(0)
        return relatorio_cruzado

    # Função para encontrar Customer Ids com registros de ambos os eventos e calcular a comissão
    def calcular_comissao(df):
        df['Event Type'] = df['Event Type'].str.strip()  # Remover espaços extras
        df['Status'] = df['Status'].str.strip()  # Remover espaços extras
        
        # Filtrar registros para 'Free Trial API', 'Paid Trial API' e 'Online Sale API' com Status 'Approved'
        df_filtered = df[(df['Event Type'].isin(['Free Trial API', 'Paid Trial API', 'Online Sale API'])) & (df['Status'] == 'Approved')]

        # Encontrar Customer Ids com pelo menos um registro de 'Paid Trial API' e um de 'Online Sale API'
        customer_ids_com_ambos = df_filtered.groupby('Customer_Id')['Event Type'].nunique()
        customer_ids_com_ambos = customer_ids_com_ambos[customer_ids_com_ambos == 3].index

        # Filtrar os registros originais para esses Customer Ids
        duplicados = df_filtered[df_filtered['Customer_Id'].isin(customer_ids_com_ambos)]

        # Calcular a comissão com base no valor de 'Paid Trial API' e 'Online Sale API'
        df_comissao = duplicados.groupby('Customer_Id').agg(
            Comissão=('Action Earnings', lambda x: x[duplicados['Event Type'] == 'Online Sale API'].sum() + x[duplicados['Event Type'] == 'Paid Trial API'].sum())
        ).reset_index()

        # Filtrar apenas os Customer Ids com comissão maior que 0
        df_comissao = df_comissao[df_comissao['Comissão'] > 0]

        # Verificar se há comissões e exibir mensagem se não houver
        if df_comissao.empty:
            print("\nNenhum Customer Id atende aos critérios para comissão (Status 'Approved').")
        else:
            # Somar toda a Comissão
            total_comissao = df_comissao['Comissão'].sum()
            print("\nComissão para Customer Ids que pagaram pelo 1 mês de trial e 1 mês cheio (Status 'Approved'):")
            print(df_comissao)
            print(f"\nTotal da Comissão para Customer Ids com status 'Approved': {total_comissao}")

            # Exportar a comissão para um arquivo CSV
            df_comissao.to_csv('comissao_customer_ids_approved.csv', index=False)
            print(f"\nArquivo 'comissao_customer_ids_approved.csv' exportado com sucesso!")

        return df_comissao

    # Função para calcular a comissão apenas para o status 'Approved'
    def calcular_comissao_approved(df):
        df['Event Type'] = df['Event Type'].str.strip()  # Remover espaços extras
        df['Status'] = df['Status'].str.strip()  # Remover espaços extras
        
        # Filtrar registros para 'Free Trial API', 'Paid Trial API' e 'Online Sale API' com Status 'Approved'
        df_filtered_approved = df[(df['Event Type'].isin(['Free Trial API', 'Paid Trial API', 'Online Sale API'])) & (df['Status'] == 'Approved')]

        # Encontrar Customer Ids com pelo menos um registro de 'Paid Trial API' e um de 'Online Sale API'
        customer_ids_com_ambos_approved = df_filtered_approved.groupby('Customer_Id')['Event Type'].nunique()
        customer_ids_com_ambos_approved = customer_ids_com_ambos_approved[customer_ids_com_ambos_approved == 3].index

        # Filtrar os registros originais para esses Customer Ids com status 'approved'
        duplicados_approved = df_filtered_approved[df_filtered_approved['Customer_Id'].isin(customer_ids_com_ambos_approved)]

        # Calcular a comissão com base no valor de 'Paid Trial API' e 'Online Sale API'
        df_comissao_approved = duplicados_approved.groupby('Customer_Id').agg(
            Comissão=('Action Earnings', lambda x: x[duplicados_approved['Event Type'] == 'Online Sale API'].sum() + x[duplicados_approved['Event Type'] == 'Paid Trial API'].sum())
        ).reset_index()

        # Filtrar apenas os Customer Ids com comissão maior que 0
        df_comissao_approved = df_comissao_approved[df_comissao_approved['Comissão'] > 0]

        # Verificar se há comissões e exibir mensagem se não houver
        if df_comissao_approved.empty:
            print("\nNenhum Customer Id atende aos critérios para comissão (Approved).")
        else:
            # Somar toda a Comissão
            total_comissao_approved = df_comissao_approved['Comissão'].sum()
            print("\nComissão para Customer Ids que pagaram pelo 1 mês de trial e 1 mês cheio (Status 'Approved'):")
            print(df_comissao_approved)
            print(f"\nTotal da Comissão para Customer Ids com status 'Approved': {total_comissao_approved}")

            # Exportar a comissão para um arquivo CSV
            df_comissao_approved.to_csv('comissao_customer_ids_approved.csv', index=False)
            print(f"\nArquivo 'comissao_customer_ids_approved.csv' exportado com sucesso!")

        return df_comissao_approved

    # Função para calcular a comissão apenas para o status 'Pending'
    def calcular_comissao_pending(df):
        df['Event Type'] = df['Event Type'].str.strip()  # Remover espaços extras
        df['Status'] = df['Status'].str.strip()  # Remover espaços extras
        
        # Filtrar registros para 'Free Trial API', 'Paid Trial API' e 'Online Sale API' com Status 'Pending'
        df_filtered_pending = df[(df['Event Type'].isin(['Free Trial API', 'Paid Trial API', 'Online Sale API'])) & (df['Status'] == 'Pending')]

        # Encontrar Customer Ids com pelo menos um registro de 'Paid Trial API' e um de 'Online Sale API'
        customer_ids_com_ambos_pending = df_filtered_pending.groupby('Customer_Id')['Event Type'].nunique()
        customer_ids_com_ambos_pending = customer_ids_com_ambos_pending[customer_ids_com_ambos_pending == 3].index

        # Filtrar os registros originais para esses Customer Ids com status 'Pending'
        duplicados_pending = df_filtered_pending[df_filtered_pending['Customer_Id'].isin(customer_ids_com_ambos_pending)]

        # Calcular a comissão com base no valor de 'Paid Trial API' e 'Online Sale API'
        df_comissao_pending = duplicados_pending.groupby('Customer_Id').agg(
            Comissão=('Action Earnings', lambda x: x[duplicados_pending['Event Type'] == 'Online Sale API'].sum() + x[duplicados_pending['Event Type'] == 'Paid Trial API'].sum())
        ).reset_index()

        # Filtrar apenas os Customer Ids com comissão maior que 0
        df_comissao_pending = df_comissao_pending[df_comissao_pending['Comissão'] > 0]

        # Verificar se há comissões e exibir mensagem se não houver
        if df_comissao_pending.empty:
            print("\nNenhum Customer Id atende aos critérios para comissão (Pending).")
        else:
            # Somar toda a Comissão
            total_comissao_pending = df_comissao_pending['Comissão'].sum()
            print("\nComissão para Customer Ids que pagaram pelo 1 mês de trial e 1 mês cheio (Status 'Pending'):")
            print(df_comissao_pending)
            print(f"\nTotal da Comissão para Customer Ids com status 'Pending': {total_comissao_pending}")

            # Exportar a comissão para um arquivo CSV
            df_comissao_pending.to_csv('comissao_customer_ids_pending.csv', index=False)
            print(f"\nArquivo 'comissao_customer_ids_pending.csv' exportado com sucesso!")

        return df_comissao_pending

    # Calcular relatórios para cada Sub Id
    relatorio_sub_id1 = calcular_faturamento(df, 'Sub Id 1')
    relatorio_sub_id2 = calcular_faturamento(df, 'Sub Id 2')
    relatorio_sub_id3 = calcular_faturamento(df, 'Sub Id 3')

    # Exportar os relatórios para arquivos CSV
    relatorio_sub_id1.to_csv('faturamento_sub_id1.csv', index=False)
    relatorio_sub_id2.to_csv('faturamento_sub_id2.csv', index=False)
    relatorio_sub_id3.to_csv('faturamento_sub_id3.csv', index=False)

    # Calcular relatório cruzado para Sub Id 1 e Sub Id 2
    relatorio_cruzado = calcular_faturamento_cruzado(df)
    relatorio_cruzado.to_csv('faturamento_cruzado.csv', index=False)

    # Calcular a comissão para Customer Ids que pagaram pelo 1 mês de trial e 1 mês cheio
    comissao = calcular_comissao(df)

    # Calcular a comissão apenas para o status 'Approved'
    comissao_approved = calcular_comissao_approved(df)

    # Calcular a comissão apenas para o status 'Pending'
    comissao_pending = calcular_comissao_pending(df)

    # Exibir os relatórios finais
    print("Relatório Sub Id 1:")
    print(relatorio_sub_id1)
    print("\nRelatório Sub Id 2:")
    print(relatorio_sub_id2)
    print("\nRelatório Sub Id 3:")
    print(relatorio_sub_id3)
    print("\nRelatório Cruzado (Sub Id 1 e Sub Id 2):")
    print(relatorio_cruzado)

except Exception as e:
    print(f"Erro ao ler o arquivo CSV: {e}")
