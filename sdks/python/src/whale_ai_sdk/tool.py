"""Python tool reflection, JSON Schema synthesis, and Reverse RPC execution."""

from __future__ import annotations

import inspect
import re
from typing import Any, Callable, Dict, List, Optional, Union, get_args, get_origin

from .types import CanonicalContent, CanonicalToolOutput


def _python_type_to_json_schema(annotation: Any) -> Dict[str, Any]:
    """Recursively converts a Python type annotation into a JSON Schema object."""
    if annotation is inspect.Parameter.empty or annotation is Any or annotation is None:
        return {}

    # If annotation is string due to `from __future__ import annotations`
    if isinstance(annotation, str):
        ann_clean = annotation.strip()
        if ann_clean in ("str", "string"):
            return {"type": "string"}
        elif ann_clean in ("int", "integer"):
            return {"type": "integer"}
        elif ann_clean in ("float", "number"):
            return {"type": "number"}
        elif ann_clean in ("bool", "boolean"):
            return {"type": "boolean"}
        elif ann_clean in ("dict", "Dict"):
            return {"type": "object"}
        elif ann_clean in ("list", "List"):
            return {"type": "array"}
        # Match list[...] / List[...]
        m_list = re.match(r"^(?:list|List)\[(.*)\]$", ann_clean)
        if m_list:
            inner = m_list.group(1).strip()
            return {"type": "array", "items": _python_type_to_json_schema(inner)}
        # Match Optional[...]
        m_opt = re.match(r"^(?:Optional)\[(.*)\]$", ann_clean)
        if m_opt:
            return _python_type_to_json_schema(m_opt.group(1).strip())
        # Match Union[..., None]
        m_union = re.match(r"^(?:Union)\[(.*)\]$", ann_clean)
        if m_union:
            types = [t.strip() for t in m_union.group(1).split(",")]
            non_none = [t for t in types if t not in ("None", "NoneType", "type(None)")]
            if len(non_none) == 1:
                return _python_type_to_json_schema(non_none[0])
            return {"anyOf": [_python_type_to_json_schema(t) for t in non_none]}
        return {"type": "string"}

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Handle Optional[T] / Union[T, None]
    if origin is Union:
        non_none_args = [a for a in args if a is not type(None)]
        if len(non_none_args) == 1:
            return _python_type_to_json_schema(non_none_args[0])
        return {"anyOf": [_python_type_to_json_schema(a) for a in non_none_args]}

    # Primitive types
    if annotation is str:
        return {"type": "string"}
    elif annotation is int:
        return {"type": "integer"}
    elif annotation is float:
        return {"type": "number"}
    elif annotation is bool:
        return {"type": "boolean"}

    # Collections: list, sequence
    if annotation in (list, tuple, set) or origin in (list, tuple, set, List):
        item_schema = _python_type_to_json_schema(args[0]) if args else {}
        return {"type": "array", "items": item_schema}

    # Dictionary / Object
    if annotation in (dict, Dict) or origin in (dict, Dict):
        val_schema = _python_type_to_json_schema(args[1]) if len(args) > 1 else {}
        schema: Dict[str, Any] = {"type": "object"}
        if val_schema:
            schema["additionalProperties"] = val_schema
        return schema

    # Fallback to string representation or empty schema
    return {"type": "string"}


def _parse_docstring(doc: Optional[str]) -> tuple[str, Dict[str, str]]:
    """Parses a docstring into a top-level description and parameter descriptions.

    Supports Google, Sphinx, and Markdown docstring styles.
    """
    if not doc:
        return "", {}

    lines = doc.strip().splitlines()
    summary_lines: List[str] = []
    param_docs: Dict[str, str] = {}

    in_args_section = False
    current_param: Optional[str] = None
    current_param_text: List[str] = []

    def flush_param() -> None:
        nonlocal current_param, current_param_text
        if current_param:
            param_docs[current_param] = " ".join(current_param_text).strip()
            current_param = None
            current_param_text = []

    for raw_line in lines:
        line = raw_line.strip()

        # Section headers
        if re.match(r"^(Args|Arguments|Parameters)\s*:", line, re.IGNORECASE):
            in_args_section = True
            flush_param()
            continue
        elif in_args_section and re.match(r"^(Returns|Raises|Example|Examples|Yields)\s*:", line, re.IGNORECASE):
            in_args_section = False
            flush_param()
            continue

        if in_args_section:
            # Match Google style: param_name (type): description OR param_name: description
            m = re.match(r"^(\w+)(?:\s*\([^)]*\))?\s*:\s*(.*)$", line)
            if m:
                flush_param()
                current_param = m.group(1)
                if m.group(2):
                    current_param_text.append(m.group(2))
            elif current_param and line:
                current_param_text.append(line)
        else:
            # Check Sphinx style :param name: description
            m_sphinx = re.match(r"^:param\s+(\w+):\s*(.*)$", line)
            if m_sphinx:
                param_docs[m_sphinx.group(1)] = m_sphinx.group(2).strip()
            else:
                summary_lines.append(line)

    flush_param()
    description = "\n".join(summary_lines).strip()
    return description, param_docs


class Tool:
    """Represents a callable tool with JSON Schema metadata for LLM invocation."""

    def __init__(
        self,
        name: str,
        description: str,
        parameters: Dict[str, Any],
        func: Callable[..., Any],
        supports_parallel: bool = True,
        require_approval: bool = False,
        is_host_tool: bool = True,
    ) -> None:
        self.name = name
        self.description = description
        self.parameters = parameters
        self.func = func
        self.supports_parallel = supports_parallel
        self.require_approval = require_approval
        self.is_host_tool = is_host_tool

    @classmethod
    def from_function(
        cls,
        func: Callable[..., Any],
        name: Optional[str] = None,
        description: Optional[str] = None,
        supports_parallel: bool = True,
        require_approval: bool = False,
    ) -> Tool:
        """Constructs a Tool from a Python function via signature and docstring reflection."""
        tool_name = name or func.__name__

        doc = inspect.getdoc(func)
        doc_desc, param_docs = _parse_docstring(doc)
        tool_description = description or doc_desc or f"Tool function '{tool_name}'"

        try:
            sig = inspect.signature(func, eval_str=True)
        except Exception:
            sig = inspect.signature(func)
        properties: Dict[str, Any] = {}
        required: List[str] = []

        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue

            param_schema = _python_type_to_json_schema(param.annotation)
            if param_name in param_docs:
                param_schema["description"] = param_docs[param_name]

            properties[param_name] = param_schema

            if param.default is inspect.Parameter.empty:
                required.append(param_name)

        parameters_schema: Dict[str, Any] = {
            "type": "object",
            "properties": properties,
        }
        if required:
            parameters_schema["required"] = required

        return cls(
            name=tool_name,
            description=tool_description,
            parameters=parameters_schema,
            func=func,
            supports_parallel=supports_parallel,
            require_approval=require_approval,
            is_host_tool=True,
        )

    def execute(self, arguments: Union[Dict[str, Any], Any]) -> CanonicalToolOutput:
        """Executes the Python function with arguments and packages the output into CanonicalToolOutput."""
        args_dict = arguments if isinstance(arguments, dict) else {}
        result = self.func(**args_dict)

        if isinstance(result, CanonicalToolOutput):
            return result
        elif isinstance(result, (dict, list)):
            return CanonicalToolOutput.from_structured(result)
        elif isinstance(result, str):
            return CanonicalToolOutput.from_text(result)
        elif result is None:
            return CanonicalToolOutput.from_structured({"status": "success", "result": None})
        else:
            return CanonicalToolOutput.from_structured({"result": result})

    def to_definition_dict(self) -> Dict[str, Any]:
        """Serializes tool into protocol RegisterToolDefinition dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "supports_parallel": self.supports_parallel,
            "require_approval": self.require_approval,
            "is_host_tool": self.is_host_tool,
        }
