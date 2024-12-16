from pathlib import Path

import jinja2


def load_sql_template(filename: str) -> str:
    """Load SQL template from file"""
    sql_dir = Path(__file__).parent.parent.parent / "include"
    with open(sql_dir / filename, "r") as f:
        return f.read()


def render_sql_template(filename: str, **kwargs) -> str:
    """Load and render SQL template with variables"""
    template = jinja2.Template(load_sql_template(filename))
    return template.render(**kwargs)
