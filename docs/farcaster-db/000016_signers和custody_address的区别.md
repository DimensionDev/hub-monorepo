# signer和custody地址有什么区别

2026-11-16，核对Farcaster官方账号、IdRegistry、KeyRegistry及KeyGateway文档，并结合当前转换字段。

custody地址可以理解成“账号管理员的钱包地址”；signer可以理解成“管理员授权应用使用的操作凭证”。它们都是可公开读取的信息。数据库signers表只保存公钥，应用真正签署消息要用对应私钥。

| 项目 | custody_address | signers里的signer |
| --- | --- | --- |
| 表示什么 | 哪个地址拥有这个FID | 哪把应用签名公钥被这个FID授权 |
| 主要用途 | 管理FID归属、授权/撤销应用密钥、参与账号转移等操作 | 应用代表用户发送帖子、关注、点赞等消息 |
| 一个人有几份 | 一个FID在某一时刻有一个当前custody地址 | 一个FID可以授权多把signer，给不同应用使用 |
| 账号控制权 | 是FID所有权的地址记录 | 授权signer不等于转移FID所有权 |
| 我们的来源 | onchain_events.type=3的注册/转移事件 | onchain_events.type=1的ADD/REMOVE/ADMIN_RESET事件 |
| 当前业务表 | fids.custody_address | signers.signer，关联用户fid和请求授权的app_fid |

例如，一个FID由钱包地址A持有，又授权公钥K给应用B。A决定账号归谁，以及是否给应用授权；应用B使用K对应的私钥签署日常消息。撤销K表示撤销该应用密钥，FID不会因此自动归应用B所有。

这里说的是角色区别。不能只看十六进制长度判断全部协议类型；转换仍保留协议原始密钥字节。

## usernames里同名字段要特别区分

`fids.custody_address` 是账号的当前持有地址。

`usernames.custody_address` 这个列名容易误解：现有Shuttle及本项目转换器实际使用的是用户名证明中的 `owner`。它记录这份证明所依据的地址，不能拿账号当前custody地址替换。对于ENS，证明地址也可能来自该账号已验证的地址；证明时间和当前时刻的账号持有地址也可能不同。

当前转换代码允许这个用户名证明地址为NULL，仍会记录字段缺口；不能用固定地址或当前账号地址补造。

## 依据

- [官方Accounts文档](https://docs.farcaster.xyz/learn/what-is-farcaster/accounts)：账号可为应用授权密钥，应用用它代发消息；每个应用通常使用各自密钥。
- [官方IdRegistry](https://docs.farcaster.xyz/reference/contracts/reference/id-registry)：custodyOf查询FID的持有地址；IdRegistry管理转移和恢复。
- [官方KeyRegistry](https://docs.farcaster.xyz/reference/contracts/reference/key-registry)：保存公钥，读取/撤销密钥；代撤销需FID owner签名。
- [官方KeyGateway](https://docs.farcaster.xyz/reference/contracts/reference/key-gateway)：新增应用公钥，代授权需FID owner签名。
- [官方协议UsernameProof规则](https://github.com/farcasterxyz/protocol/blob/main/docs/SPECIFICATION.md)：ENS证明owner与custody或已验证地址的关系。
- [Shuttle用户名证明转换](https://github.com/DimensionDev/hub-monorepo/blob/main/packages/shuttle/src/shuttle/usernameProofReconciliation.ts)：从usernames.custodyAddress还原proof.owner。
