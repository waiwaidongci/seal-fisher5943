"""工艺计数浮点存储 → 整数专用存储迁移的阶段定义。

阶段机：
  legacy    迁移未开始；旧 JSON 浮点表唯一可读写。
  dual      在线回填 + 双写；读取依据仍为旧表，新增/更正/移除同时落两存储。
  switching 原子切换进行中；禁止发起新的研究冻结，写入仍可继续（双写）。
  integer   切换完成；整数表为读取依据，双写旧表以保留回退通道。
回退：integer/switching → dual，期间在整数表上的写入全部保留。
"""
LEGACY='legacy'
DUAL='dual'
SWITCHING='switching'
INTEGER='integer'
PHASES=(LEGACY,DUAL,SWITCHING,INTEGER)

READ_BASIS={
    LEGACY:'旧浮点存储 observations',
    DUAL:'旧浮点存储 observations（整数表在线回填中）',
    SWITCHING:'旧浮点存储 observations（正在原子切换）',
    INTEGER:'整数专用存储 observations_int',
}

def basis(phase):
    return READ_BASIS.get(phase,phase)
