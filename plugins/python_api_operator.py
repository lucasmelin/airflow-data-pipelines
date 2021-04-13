from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from jinja2 import Template, Environment, FileSystemLoader
from pathlib import Path


class GenerateAPIOperator(BaseOperator):

    template_fields = ['dest']

    @apply_defaults
    def __init__(
            self,
            dest: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.dest = dest

    def execute(self, context):
        p = '/opt/airflow/templates' 
        env = Environment(
            loader=FileSystemLoader(Path(p))
        )
        name = Path(context['task_instance'].xcom_pull(task_ids='get_filename'))
        destination = Path(self.dest)
        if not destination.is_file():
            template = env.get_template('python_api_skeleton.jinja2')
            result = template.render(name=name.stem)
            with open(self.dest, "w") as fh:
                fh.write(result)
        else:
            template = env.get_template('python_api.jinja2')
            result = template.render(name=name.stem)
            with open(self.dest, "r+") as fh:
                if name.stem not in fh.read():
                   fh.write(result)
        return self.dest
