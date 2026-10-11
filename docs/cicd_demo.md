# CI/CD 演示 — 以服务主体身份部署 (M5, 约 10 分钟)

# CI/CD demo — deploy as a service principal (M5, about 10 minutes)

**要点：** 在生产环境中，没有人手动部署。合并到默认分支会运行 GitLab CI，它以服务主体身份部署包。作业和管道由服务主体拥有并运行，人员只获得查看权限。这是 Hytech 的"无直接生产访问"优先级，而*实现 CI/CD* 占数据工程师认证考试的 10%。

**Message:** in production nobody deploys by hand. A merge to the default branch runs GitLab CI, which deploys
the bundle as a service principal. The jobs and the pipeline are owned by and run as that service principal,
and people get view rights only. This is Hytech's "no direct prod access" priority, and *Implementing CI/CD* is
10% of the Data Engineer Associate exam.

## 演示什么 (What to show)

1. **`databricks.yml`：一个包，三个目标**<br>**`databricks.yml`: one bundle, three targets**

   | 目标 (Target) | 模式 (Mode) | 部署者 (Who deploys) | 说明 (Notes) |
   |---|---|---|---|
   | `dev` | 开发 (development) | 每个工程师 (each engineer) | 名称前缀 `[dev <you>]`，计划和触发器已暂停，自己的副本<br>Names prefixed `[dev <you>]`, schedules and triggers paused, own copy |
   | `prod` | 生产 (production) | 工作坊管理员 (workshop admin) | 共享工作坊部署（根路径在 `/Workspace/Shared` 中，以便多个管理员可以重新部署同一副本）<br>The shared workshop deployment (root path in `/Workspace/Shared` so several admins can redeploy one copy) |
   | `cicd` | 生产 (production) | **仅服务主体** (**service principal only**) | 以服务主体身份 `run_as`，根路径在其主文件夹中，`users` 获得 CAN_VIEW，名称前缀 `[cicd]`，自己的 schema `solutions_cicd`<br>`run_as` the service principal, root path in its home folder, `users` get CAN_VIEW, names prefixed `[cicd]`, own schema `solutions_cicd` |

2. **`cicd/gitlab-ci.yml`：** 在合并请求上进行 `validate`，在默认分支上进行 `deploy`。凭证被掩盖和受保护的 CI 变量。`resource_group` 允许一次一个部署。<br>**`cicd/gitlab-ci.yml`:** `validate` on merge requests and `deploy` on the default branch. Credentials are masked and protected CI variables. `resource_group` allows one deployment at a time.
3. **以服务主体身份实时部署**，从笔记本电脑上使用与 CI 作业相同的命令：

   ```bash
   PROFILE=<admin CLI profile> ./cicd/deploy_as_service_principal.sh
   ```

   该脚本创建一个临时 OAuth 密钥并隐藏你自己的 CLI 配置文件。然后它以服务主体身份运行 `bundle validate`（`User:` 显示应用程序 ID，而不是个人）、`bundle deploy` 和 `bundle run`，并在退出时删除密钥，即使在失败的情况下也是如此。部署耗时约一分钟，作业耗时约四分钟。

**Live deploy as the service principal** from the laptop, with the same commands the CI job runs:

   ```bash
   PROFILE=<admin CLI profile> ./cicd/deploy_as_service_principal.sh
   ```

   The script creates a temporary OAuth secret and hides your own CLI profiles. It then runs `bundle validate`
   (`User:` shows the application ID, not a person), `bundle deploy` and `bundle run` as the service principal,
   and deletes the secret when it exits, even on failure. Deploying takes about a minute and the job about four.

4. **在工作区中：** 打开作业 `[cicd] hytech_daily_trading_reporting_solution`。*Run as* 和所有者是服务主体。学员可以查看但不能编辑。已部署的文件位于服务主体的主文件夹中。<br>**In the workspace:** open job `[cicd] hytech_daily_trading_reporting_solution`. *Run as* and owner are the service principal. Participants can view it but not edit it. The deployed files sit in the service principal's home folder.

## 演示工作区的一次性设置（讲师，约 10 分钟）

## One-time setup on the demo workspace (instructor, about 10 minutes)

1. **创建服务主体**（工作区管理员）：<br>**Create the service principal** (workspace admin):

   ```bash
   databricks service-principals create -p <profile> \
     --json '{"displayName": "hytech-ws-cicd", "entitlements": [{"value": "workspace-access"}]}'
   ```

   记下 `id`（用于密钥）和 `applicationId`（用于授权和 `DATABRICKS_CLIENT_ID`）。

   Note `id` (for the secret) and `applicationId` (for grants and `DATABRICKS_CLIENT_ID`).
2. **OAuth 密钥，仅用于 GitLab：** *Settings → Identity and access → Service principals → hytech-ws-cicd → Secrets → Generate secret*。它只显示一次；仅将其存储为掩盖的受保护 CI 变量。演示脚本创建并删除自己的临时密钥，因此演示工作区不保留任何密钥。<br>**OAuth secret, only for GitLab:** *Settings → Identity and access → Service principals → hytech-ws-cicd → Secrets → Generate secret*. It is shown once; store it only as a masked, protected CI variable. The demo script creates and deletes its own temporary secret, so the demo workspace keeps none.
3. **Unity Catalog 授权**（SQL 编辑器，以 catalog 所有者身份）：<br>**Unity Catalog grants** (SQL editor, as the catalog owner):

   ```sql
   CREATE SCHEMA IF NOT EXISTS hytech_ws_catalog.solutions_cicd
     COMMENT 'CI/CD demo: owned and written by the service principal';
   ALTER SCHEMA hytech_ws_catalog.solutions_cicd OWNER TO `<application-id>`;
   GRANT USE CATALOG ON CATALOG hytech_ws_catalog TO `<application-id>`;
   GRANT USE SCHEMA ON SCHEMA hytech_ws_catalog.raw TO `<application-id>`;
   GRANT READ VOLUME ON VOLUME hytech_ws_catalog.raw.landing TO `<application-id>`;
   GRANT READ VOLUME ON VOLUME hytech_ws_catalog.raw.ref TO `<application-id>`;
   ```

   服务主体拥有其 schema，因为生产对象应由服务主体或组（而不是个人）拥有。它只需要对着陆区的读权限。

   The service principal owns its schema, as production objects should be owned by a service principal or a
   group rather than a person. It only needs read access to the landing zone.
4. **在工作坊前做一次部署和一次运行**（*演示什么*的第 3 步）并检查运行是否为绿色。<br>**Do one deploy and one run before the workshop** (step 3 of *What to show*) and check the run is green.

## 来自 FEVM 预演的笔记（2026 年 10 月 3 日）

## Notes from the FEVM dry run (3 Oct 2026)

- `hytech-ws-cicd` 通过 OAuth M2M 部署并运行了 `cicd` 目标。作业和管道由它拥有并以它身份运行，`users` 有 CAN VIEW，`solutions_cicd` 中的所有 23 张表都由它拥有。运行在约 3.5 分钟内完成并为绿色。
  `hytech-ws-cicd` deployed and ran the `cicd` target over OAuth M2M. The job and pipeline are owned by it and
  run as it, `users` have CAN VIEW, and all 23 tables in `solutions_cicd` are owned by it. The run was green
  in about 3.5 minutes.

- 服务主体主文件夹中的根路径无需先创建文件夹即可工作。
  The root path in the service principal's home folder works without creating the folder first.

- 即使 catalog 所有者在服务主体的 schema 上也会获得 `INSUFFICIENT_PERMISSIONS`：在有人授权前没有 `USE SCHEMA`。这是最小特权原则的实际应用，也是一个很好的讨论点。要显示数据，catalog 所有者可以向只读组授予 `solutions_cicd` 上的 `USE SCHEMA` 和 `SELECT`。
  Even the catalog owner gets `INSUFFICIENT_PERMISSIONS` on the service principal's schema: no `USE SCHEMA`
  until someone grants it. This is least privilege in action and a good talking point. To show the data, the
  catalog owner can grant `USE SCHEMA` and `SELECT` on `solutions_cicd` to a read-only group.

## 对于 Hytech（Triones / Robin）

## For Hytech (Triones / Robin)

- 为每个环境使用一个服务主体（例如 `sp-de-prod`）并具有工作区访问权限，并仅在掩盖的受保护 GitLab 变量中保留其 OAuth 密钥。轮换密钥。
  Use one service principal per environment (for example `sp-de-prod`) with workspace access, and keep its
  OAuth secret only in masked, protected GitLab variables. Rotate the secret.

- 服务主体拥有生产 schema；人员获得读访问权限。
  The service principal owns the production schemas; people get read access.

- 保护默认分支，要求合并请求批准，并在每个合并请求上运行 `bundle validate`。
  Protect the default branch, require merge-request approval, and run `bundle validate` on every merge request.

- 在 `databricks.yml` 中为每个目标设置 `workspace.host`（主机不能来自变量）。
  Set `workspace.host` per target in `databricks.yml` (the host cannot come from a variable).

- 中国内地的 Runner 可能无法访问 github.com：将 Databricks CLI 烘焙到 Runner 镜像中。
  Runners in mainland China may not reach github.com: bake the Databricks CLI into the runner image.
