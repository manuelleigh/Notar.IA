
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML
import os

TEMPLATE_DIR = os.getenv("TEMPLATE_DIR", "./templates")

env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))

def render_html(template_name: str, context: dict) -> str:
    template = env.get_template(template_name)
    return template.render(**context)

def html_to_pdf(html_content: str, output_path: str):
    HTML(string=html_content).write_pdf(output_path)
    return output_path
