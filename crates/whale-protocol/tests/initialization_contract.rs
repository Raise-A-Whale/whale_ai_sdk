use serde_json::json;
use whale_protocol::initialization::*;

fn request() -> InitializeParams {
    InitializeParams::sdk("whale-test", "0.1.0")
}
fn server() -> PeerInfo {
    PeerInfo {
        name: "custom-daemon".into(),
        version: "0.1.0".into(),
    }
}

#[test]
fn negotiates_offered_version_and_reports_real_features() {
    let mut params = request();
    params.protocol_versions = vec![2, 1];
    let result = InitializeResult::negotiate(&params, server()).unwrap();
    assert_eq!(result.protocol_version, 1);
    assert_eq!(result.capabilities, PROTOCOL_CAPABILITIES);
    result.validate_for(&params).unwrap();
    let encoded = serde_json::to_value(&params).unwrap();
    assert_eq!(encoded["client"]["name"], "whale-test");
    assert_eq!(encoded["protocol_versions"], json!([2, 1]));
}

#[test]
fn rejects_incompatible_version_or_required_feature() {
    let mut params = request();
    params.protocol_versions = vec![2];
    assert!(InitializeResult::negotiate(&params, server()).is_err());
    params.protocol_versions = vec![1];
    params.required_capabilities.push("future.v2".into());
    assert!(InitializeResult::negotiate(&params, server()).is_err());
}

#[test]
fn validates_names_versions_and_capability_sets() {
    for versions in [vec![], vec![0], vec![1, 1]] {
        let mut params = request();
        params.protocol_versions = versions;
        assert!(params.validate().is_err());
    }
    for caps in [vec![""], vec![" runs.v1"], vec!["runs.v1", "runs.v1"]] {
        let mut params = request();
        params.required_capabilities = caps.into_iter().map(str::to_owned).collect();
        assert!(params.validate().is_err());
    }
    for name in ["", "  ", " client"] {
        let mut params = request();
        params.client.name = name.into();
        assert!(params.validate().is_err());
    }
    let mut params = request();
    params.required_capabilities.clear();
    assert!(params.validate().is_ok());
    assert!(InitializeResult::negotiate(&params, server()).is_ok());
}

#[test]
fn response_cannot_select_unoffered_version_or_omit_required_feature() {
    let params = request();
    let valid = InitializeResult::negotiate(&params, server()).unwrap();
    let mut wrong_version = valid.clone();
    wrong_version.protocol_version = 2;
    assert!(wrong_version.validate_for(&params).is_err());
    let mut missing = valid.clone();
    missing.capabilities.pop();
    assert!(missing.validate_for(&params).is_err());
    let mut extra = valid;
    extra.capabilities.push("future.v2".into());
    assert!(extra.validate_for(&params).is_ok());
}

#[test]
fn wire_rejects_boolean_float_string_negative_and_oversized_versions() {
    for value in [
        json!(true),
        json!(1.0),
        json!("1"),
        json!(-1),
        json!(4294967296u64),
    ] {
        let mut params = serde_json::to_value(request()).unwrap();
        params["protocol_versions"] = json!([value]);
        assert!(serde_json::from_value::<InitializeParams>(params).is_err());
        let mut result =
            serde_json::to_value(InitializeResult::negotiate(&request(), server()).unwrap())
                .unwrap();
        result["protocol_version"] = value;
        assert!(serde_json::from_value::<InitializeResult>(result).is_err());
    }
}

#[test]
fn shared_language_response_cases_match_rust_contract() {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/initialization-v1.json"
    ))
    .unwrap();
    let params: InitializeParams = serde_json::from_value(fixture["request"].clone()).unwrap();
    let cases = fixture["response_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 24);
    for case in cases {
        let valid = serde_json::from_value::<InitializeResult>(case["result"].clone())
            .ok()
            .is_some_and(|result| result.validate_for(&params).is_ok());
        assert_eq!(
            valid,
            case["valid"].as_bool().unwrap(),
            "case {}",
            case["name"]
        );
    }
}
