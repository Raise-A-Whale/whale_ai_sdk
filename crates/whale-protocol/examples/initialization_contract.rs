//! Regenerate the shared bootstrap fixture/schema, or use --check in validation.
use serde_json::{json, Value};
use std::path::Path;
use whale_protocol::initialization::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let check = match std::env::args().skip(1).collect::<Vec<_>>().as_slice() {
        [] => false,
        [flag] if flag == "--check" => true,
        _ => return Err("Usage: initialization_contract [--check]".into()),
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let request = InitializeParams::sdk("whale-contract", "0.1.0");
    let response = serde_json::to_value(InitializeResult::negotiate(
        &request,
        PeerInfo {
            name: "whale-daemon".into(),
            version: "0.1.0".into(),
        },
    )?)?;
    let mut cases = vec![json!({"name": "current", "result": response, "valid": true})];
    let mut add = |name: &str, valid: bool, edit: fn(&mut Value)| {
        let mut result = response.clone();
        edit(&mut result);
        cases.push(json!({"name": name, "result": result, "valid": valid}));
    };
    add("additional_capability", true, |r| {
        r["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!("future.v2"));
    });
    add("additional_fields", true, |r| {
        r["future"] = json!({"enabled": true});
        r["server"]["future"] = json!(1);
    });
    add("custom_server_name", true, |r| {
        r["server"]["name"] = json!("application-daemon")
    });
    add("wrong_version", false, |r| r["protocol_version"] = json!(2));
    add("boolean_version", false, |r| {
        r["protocol_version"] = json!(true)
    });
    add("floating_version", false, |r| {
        r["protocol_version"] = json!(1.0)
    });
    add("string_version", false, |r| {
        r["protocol_version"] = json!("1")
    });
    add("zero_version", false, |r| r["protocol_version"] = json!(0));
    add("negative_version", false, |r| {
        r["protocol_version"] = json!(-1)
    });
    add("oversized_version", false, |r| {
        r["protocol_version"] = json!(4294967296u64)
    });
    add("null_version", false, |r| {
        r["protocol_version"] = Value::Null
    });
    add("missing_version", false, |r| {
        r.as_object_mut().unwrap().remove("protocol_version");
    });
    add("missing_capability", false, |r| {
        r["capabilities"].as_array_mut().unwrap().pop();
    });
    add("duplicate_capability", false, |r| {
        r["capabilities"]
            .as_array_mut()
            .unwrap()
            .push(json!("runs.v1"));
    });
    add("blank_capability", false, |r| {
        r["capabilities"].as_array_mut().unwrap().push(json!(" "));
    });
    add("nonstring_capability", false, |r| {
        r["capabilities"].as_array_mut().unwrap().push(json!(true));
    });
    add("missing_capabilities", false, |r| {
        r.as_object_mut().unwrap().remove("capabilities");
    });
    add("blank_server_name", false, |r| {
        r["server"]["name"] = json!("")
    });
    add("padded_server_name", false, |r| {
        r["server"]["name"] = json!(" daemon")
    });
    add("blank_server_version", false, |r| {
        r["server"]["version"] = json!("")
    });
    add("nonstring_server_version", false, |r| {
        r["server"]["version"] = json!(1)
    });
    add("missing_server", false, |r| {
        r.as_object_mut().unwrap().remove("server");
    });
    add("nonobject_result", false, |r| *r = json!([]));
    let fixture =
        json!({"protocol_version": PROTOCOL_VERSION, "request": request, "response_cases": cases});
    let schemas = json!({
        "protocol_version": PROTOCOL_VERSION,
        "note": "Structural schemas; negotiation and semantic set/name checks are specified by initialization-v1.json and InitializeResult::validate_for.",
        "initialize_params": schemars::schema_for!(InitializeParams),
        "initialize_result": schemars::schema_for!(InitializeResult),
    });
    for (relative, value) in [
        ("fixtures/protocol/initialization-v1.json", fixture),
        ("docs/protocol/initialization-v1.schema.json", schemas),
    ] {
        let path = root.join(relative);
        let text = serde_json::to_string_pretty(&value)? + "\n";
        if check {
            if std::fs::read_to_string(&path)? != text {
                return Err(format!("Generated contract is stale: {relative}").into());
            }
        } else {
            std::fs::create_dir_all(path.parent().unwrap())?;
            std::fs::write(&path, text)?;
        }
        println!("{} {relative}", if check { "Checked" } else { "Generated" });
    }
    Ok(())
}
