import pandas as pd
import logging

def transformation_ej_profiles_and_ej_users(dfs):
    df_profile = dfs[0]
    df_users = dfs[1]
    results = []

    # Criar um DataFrame vazio com as colunas necessárias
    df_result_transformation = pd.DataFrame(columns=[
        'user_id', 'phone_number', 'password', 'last_login', 
        'is_superuser', 'is_staff', 'is_active', 'data_joined', 
        'name', 'email', 'region', 'ethnicity', 'gender', 
        'age_range', 'secret_id'
    ])

    for _, data_users in df_users.iterrows():
        for _, data_profiles in df_profile.iterrows():
            if data_users['id'] == data_profiles['user_id']:  # Certifique-se de que está usando as chaves corretas

                # Criar um dicionário com os dados da linha
                new_row = {
                    'user_id': data_users['id'],
                    'phone_number': data_profiles['phone_number'],
                    'password': data_users['password'],
                    'last_login': data_users['last_login'],
                    'is_superuser': data_users['is_superuser'],
                    'is_staff': data_users['is_staff'],
                    'is_active': data_users['is_active'],
                    'data_joined': data_users['date_joined'],
                    'name': data_users['name'],
                    'email': data_users['email'],
                    'region': data_profiles['region'],
                    'ethnicity': data_profiles['ethnicity'],
                    'gender': data_profiles['gender'],
                    'age_range': data_profiles['age_range'],
                    'secret_id': data_users['secret_id']
                }
                
                # Adiciona a nova linha ao DataFrame resultante
                results.append(new_row)

    df_result_transformation = pd.DataFrame(results)
    logging.info(f"Transformação terminada com sucesso!")

    return df_result_transformation
