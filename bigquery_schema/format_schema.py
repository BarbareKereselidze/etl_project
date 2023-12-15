from google.cloud import bigquery  # :TODO bigquery client dependency is not present in the requirements.txt file


class SchemaFormatter:
    @staticmethod
    def format_schema(schema: list) -> bigquery.SchemaField:
        """ formats a schema to create a list of `bigquery.SchemaField` objects """

        formatted_schema = []

        # iterate through each row in the schema
        for row in schema:
            if 'fields' in row:
                # if the row has nested fields, recursively format the sub-schema
                sub_schema = SchemaFormatter.format_schema(row['fields'])
                formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode'], fields=sub_schema))
            else:
                # if the row doesn't have nested fields, create a SchemaField directly
                formatted_schema.append(bigquery.SchemaField(row['name'], row['type'], row['mode']))

        return formatted_schema
