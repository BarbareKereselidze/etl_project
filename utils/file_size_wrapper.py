def get_file_size_in_mb(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        if isinstance(result, list):
            result = result[0]
            for key, value in result.items():
                result[key] = round(value / (1024 ** 2), 3)
            return [result]
        return round(result / (1024 ** 2), 3)

    return wrapper
