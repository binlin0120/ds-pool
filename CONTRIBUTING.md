# 贡献指南

感谢考虑为 ds-pool 贡献代码。项目定位是**自用系统**，代码质量以「能稳定跑」为先，
不是标准开源库；功能取舍会偏向实际使用场景，欢迎 fork 自用，也欢迎有意义的改进。

## 提 Issue

- 先看 [README](./README.md) 和已有 Issue，避免重复提交。
- Bug 报告请尽量提供：版本 / 部署方式、复现步骤、脱敏后的日志与请求样例。
- **绝不**在 Issue 中张贴真实 token、密码、邮箱、cookie、服务器 IP。

## 本地开发

- 网关、探针均为 Python；本地单测见 `tests/`，改动后请跑一遍：
  - `python -m unittest discover -s tests`（以仓库实际用法为准）
- 涉及 `pool.py` / `dsess.py` / `probe_login.py` 的行为改动，请在 PR 里描述测试方式和结果。

## 敏感信息红线

- `_creds/`、`*.env`、`pool.env`、`server_ssh.env` 等一律**不入库**（已在 `.gitignore` 中）。
- 新增配置样例必须用占位符，如 `pool/pool.env.example`。
- 提交前自查：`git status` 里不应出现任何含凭据、IP、真实账号的文件。

## 提交规范

- 一个提交只做一件事，信息用语义前缀，与现有历史保持一致，例如：
  - `feat: 描述`
  - `fix: 描述`
  - `docs: 描述`
  - `chore: 描述`
- 接口、配置项变化请同步 README 和 `pool.env.example`。

## 提 PR

- 从 `master` 拉新分支，改动保持最小。
- 按 `.github/PULL_REQUEST_TEMPLATE.md` 填写，并把自查清单勾完。
- 涉及行为改动时，附上本地验证结果或测试输出。
