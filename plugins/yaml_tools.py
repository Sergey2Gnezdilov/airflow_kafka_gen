import yaml


def parse_config(config_path):
    config_dict, config_dags = {}, []
    with open(config_path, "r") as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    if len(config_dict) == 0:
        return config_dags

    default_config = config_dict.pop("defaults")
    for key in config_dict.keys():
        config_dags.append({"dag_id": key, **default_config, **config_dict[key]})

    return config_dags
