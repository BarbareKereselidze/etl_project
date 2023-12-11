table_schema = [
    {
        'name': 'csv_file_name',
        'type': 'STRING',
        'mode': 'REQUIRED',
    },
    {
        'name': 'created_at',
        'type': 'DATETIME',
        'mode': 'REQUIRED',
    },
    {
        'name': 'modified_at',
        'type': 'DATETIME',
        'mode': 'REQUIRED',
    },
    {
        'name': 'csv_file_info',
        'type': 'RECORD',
        'mode': 'REPEATED',
        'fields': [
            {
                'name': 'csv_file_size_in_mb',
                'type': 'FLOAT64',
                'mode': 'REQUIRED'
            },
            {
                'name': 'df_of_csv_rows_n',
                'type': 'INT64',
                'mode': 'REQUIRED'
            },
            {
                'name': 'df_of_csv_columns_n',
                'type': 'INT64',
                'mode': 'REQUIRED'
            },
            {
                'name': 'df_size_in_mb',
                'type': 'FLOAT64',
                'mode': 'REQUIRED'
            },
            {
                'name': 'df_of_column_size_in_mb',
                'type': 'FLOAT64',
                'mode': 'REQUIRED'
            },
            {
                'name': 'df_of_all_column_size_in_mb',
                'type': 'RECORD',
                'mode': 'REPEATED',
                'fields': [
                    {
                        'name': 'name',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'birthdate',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'email',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'address',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'phone_number',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'job',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'company',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'salary',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'credit_card_number',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    },
                    {
                        'name': 'credit_card_expire',
                        'type': 'FLOAT64',
                        'mode': 'NULLABLE'
                    }
                ]
            }
        ]
    }
]
