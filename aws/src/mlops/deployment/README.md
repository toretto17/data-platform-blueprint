# 🚀 deployment — ☁️ AWS

Deploy models safely: canary (traffic split) with instant rollback.

## Files

- `deployment.py`

## Quick usage

```python
deployer = ModelDeploymentAWS(cfg)
deployer.deploy_canary()     # 10% new model
# monitor... then:
deployer.promote_canary()    # 100% new
# OR: deployer.rollback()   # instant revert
```

## Related runbook

[📖 Full guide: HOWTO_DEPLOY_REALTIME_ENDPOINT](../../docs/runbooks/HOWTO_DEPLOY_REALTIME_ENDPOINT.md)

---

> 🔄 **Platform twin:** `./databricks/src/mlops/deployment/`
