# Эмпирический Байес: акция → ячейка (выгода/порог) → семейство (сегмент × механика × длительность) → механика → канал.
# Единица — «₽ на клиента ЦГ», вес в средних — клиенты (Σ Доп ТО / Σ клиентов), разбросы — DerSimonian–Laird.
import numpy as np, pandas as pd


def dl_tau2(groups):
    """DerSimonian–Laird по нескольким группам: groups — список (y, v); τ² общий для групп."""
    Q = df_ = C = 0.0
    for y, v in groups:
        y, v = np.asarray(y, float), np.asarray(v, float)
        m = np.isfinite(y) & np.isfinite(v) & (v > 0)
        y, v = y[m], v[m]
        if len(y) < 2:
            continue
        w = 1 / v
        yb = (w * y).sum() / w.sum()
        Q += (w * (y - yb) ** 2).sum(); df_ += len(y) - 1; C += w.sum() - (w ** 2).sum() / w.sum()
    return (max(0.0, (Q - df_) / C) if C > 0 else np.nan), Q, df_


def pooled(N, y, var):
    S = N.sum()
    return (y * N).sum() / S, ((N ** 2) * var).sum() / S ** 2


def shrink(r, v, prior, vprior, tau2):
    B = tau2 / (tau2 + v) if tau2 + v > 0 else 1.0
    return prior + B * (r - prior), B * v + (1 - B) ** 2 * vprior, B


def eb(u, cell, fam, mech):
    u = u.copy()
    key = lambda cols: u[cols].astype(str).agg(' | '.join, axis=1)
    u['kc'], u['kf'], u['km'] = key(cell), key(fam), key(mech)
    # 1) разброс между акциями одной ячейки — по механике (у механик с разным исполнением он разный), иначе общий
    tw_all, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in u.groupby('kc')])
    tw = {}
    for km, gm in u.groupby('km'):
        t, Q, d = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in gm.groupby('kc')])
        tw[km] = t if (np.isfinite(t) and d >= 2) else tw_all
    u['tw'] = u.km.map(tw)
    u['vu'] = u.s ** 2 + u.tw
    # 2) ячейки
    C = pd.DataFrame([dict(kc=k, kf=g.kf.iloc[0], km=g.km.iloc[0], n=len(g), N=g.N.sum(),
                           r=pooled(g.N, g.y, g.vu)[0], v=pooled(g.N, g.y, g.vu)[1], v0=pooled(g.N, g.y, g.s ** 2)[1],
                           tw=g.tw.iloc[0]) for k, g in u.groupby('kc')])
    tc, _, _ = dl_tau2([(g.r.values, g.v.values) for _, g in C.groupby('kf')]); tc = 0.0 if not np.isfinite(tc) else tc
    # 3) семейства
    F = pd.DataFrame([dict(kf=k, km=g.km.iloc[0], nf=len(g), ncell=g.kc.nunique(),
                           rf=pooled(g.N, g.y, g.vu + tc)[0], vf=pooled(g.N, g.y, g.vu + tc)[1]) for k, g in u.groupby('kf')])
    tf, _, _ = dl_tau2([(g.rf.values, g.vf.values) for _, g in F.groupby('km')]); tf = 0.0 if not np.isfinite(tf) else tf
    # 4) механики
    M = pd.DataFrame([dict(km=k, nm=len(g), nfam=g.kf.nunique(), npromo=g.ID.nunique(),
                           rm=pooled(g.N, g.y, g.vu + tc + tf)[0], vm=pooled(g.N, g.y, g.vu + tc + tf)[1]) for k, g in u.groupby('km')])
    tm, _, _ = dl_tau2([(M.rm.values, M.vm.values)]); tm = 0.0 if not np.isfinite(tm) else tm
    mu, vmu = pooled(u.N, u.y, u.vu + tc + tf + tm)
    # сверху вниз
    M[['pm', 'vpm', 'Bm']] = M.apply(lambda r: pd.Series(shrink(r.rm, r.vm, mu, vmu, tm)), axis=1)
    F = F.merge(M, on='km')
    F[['pf', 'vpf', 'Bf']] = F.apply(lambda r: pd.Series(shrink(r.rf, r.vf, r.pm, r.vpm, tf) if r.nfam > 1 else (r.pm, r.vpm, 1.0)), axis=1)
    C = C.merge(F[['kf', 'pf', 'vpf', 'Bf', 'rf', 'ncell', 'nf', 'rm', 'pm', 'Bm']], on='kf')
    C[['post', 'vpost', 'Bc']] = C.apply(lambda r: pd.Series(shrink(r.r, r.v, r.pf, r.vpf, tc) if r.ncell > 1 else (r.pf, r.vpf, 1.0)), axis=1)
    C['sd'] = np.sqrt(C.vpost)
    C['lo'], C['hi'], C['z'] = C.post - 1.96 * C.sd, C.post + 1.96 * C.sd, C.post / C.sd
    par = dict(tau_promo_all=np.sqrt(tw_all), tau_promo_by_mech={k: round(float(np.sqrt(v)), 2) for k, v in tw.items()},
               tau_cell=np.sqrt(tc), tau_fam=np.sqrt(tf), tau_mech=np.sqrt(tm), mu=mu, se_mu=np.sqrt(vmu))
    return C, F, par, u


def sig_label(z):
    z = abs(z)
    if z >= 3.29: return 'очень сильно значимо (p < 0,001)'
    if z >= 2.576: return 'сильно значимо (p < 0,01)'
    if z >= 1.96: return 'значимо (p < 0,05)'
    if z >= 1.645: return 'слабо значимо (p ≈ 0,05–0,10)'
    return 'неотличимо от нуля (p > 0,10)'


def eb2(u, cell, fam):
    """Два уровня: акция → ячейка (сегмент × выгода × порог) → семейство (канал × механика × длительность).
    Разброс между акциями одной ячейки τw — по семейству (DL по ячейкам с ≥2 акциями), если степеней свободы < 2 — общий по каналу.
    Ячейку тянет к среднему семейства с весом B = τc² / (τc² + V), V = ошибка выборки + τw² / эффективное число акций."""
    u = u.copy()
    key = lambda cols: u[cols].astype(str).agg(' | '.join, axis=1)
    u['kc'], u['kf'] = key(cell), key(fam)
    tw_all, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in u.groupby('kc')])
    tw = {}
    for kf, gm in u.groupby('kf'):
        t, Q, d = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in gm.groupby('kc')])
        tw[kf] = t if (np.isfinite(t) and d >= 2) else tw_all
    u['tw'] = u.kf.map(tw)
    u['vu'] = u.s ** 2 + u.tw
    C = pd.DataFrame([dict(kc=k, kf=g.kf.iloc[0], n=len(g), N=g.N.sum(),
                           r=pooled(g.N, g.y, g.vu)[0], v=pooled(g.N, g.y, g.vu)[1], v0=pooled(g.N, g.y, g.s ** 2)[1],
                           tw=g.tw.iloc[0]) for k, g in u.groupby('kc')])
    tc, Qc, dfc = dl_tau2([(g.r.values, g.v.values) for _, g in C.groupby('kf')]); tc = 0.0 if not np.isfinite(tc) else tc
    F = pd.DataFrame([dict(kf=k, nf=len(g), ncell=g.kc.nunique(),
                           rf=pooled(g.N, g.y, g.vu + tc)[0], vf=pooled(g.N, g.y, g.vu + tc)[1]) for k, g in u.groupby('kf')])
    C = C.merge(F, on='kf')
    C[['post', 'vpost', 'B']] = C.apply(lambda r: pd.Series(shrink(r.r, r.v, r.rf, r.vf, tc) if r.ncell > 1 else (r.r, r.v, 1.0)), axis=1)
    C['sd'] = np.sqrt(C.vpost)
    C['lo'], C['hi'], C['z'] = C.post - 1.96 * C.sd, C.post + 1.96 * C.sd, C.post / C.sd
    par = dict(tau_promo_all=float(np.sqrt(tw_all)), tau_promo_by_fam={k: round(float(np.sqrt(v)), 2) for k, v in tw.items()},
               tau_cell=float(np.sqrt(tc)), Qc=Qc, dfc=dfc)
    return C, F, par, u


def eb3(u, cell, fam, mech):
    """Акция → ячейка (выгода/порог) → семейство (сегмент × механика × длительность) → механика × длительность.
    τw (между акциями ячейки) — по механике; τc (между ячейками семейства) — общий; τf (между сегментами) — по механике,
    если в ней ≥ 3 семейства, иначе общий. Механику с одним семейством тянем к среднему канала с τm (между механиками)."""
    u = u.copy()
    key = lambda cols: u[cols].astype(str).agg(' | '.join, axis=1)
    u['kc'], u['kf'], u['km'] = key(cell), key(fam), key(mech)
    tw_all, _, _ = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in u.groupby('kc')])
    tw = {}
    for km, gm in u.groupby('km'):
        t, Q, d = dl_tau2([(g.y.values, (g.s ** 2).values) for _, g in gm.groupby('kc')])
        tw[km] = t if (np.isfinite(t) and d >= 2) else tw_all
    u['tw'] = u.km.map(tw); u['vu'] = u.s ** 2 + u.tw
    C = pd.DataFrame([dict(kc=k, kf=g.kf.iloc[0], km=g.km.iloc[0], n=len(g), N=g.N.sum(),
                           r=pooled(g.N, g.y, g.vu)[0], v=pooled(g.N, g.y, g.vu)[1], v0=pooled(g.N, g.y, g.s ** 2)[1],
                           tw=g.tw.iloc[0]) for k, g in u.groupby('kc')])
    tc, _, _ = dl_tau2([(g.r.values, g.v.values) for _, g in C.groupby('kf')]); tc = 0.0 if not np.isfinite(tc) else tc
    F = pd.DataFrame([dict(kf=k, km=g.km.iloc[0], nf=len(g), ncell=g.kc.nunique(),
                           rf=pooled(g.N, g.y, g.vu + tc)[0], vf=pooled(g.N, g.y, g.vu + tc)[1]) for k, g in u.groupby('kf')])
    tf_all, _, _ = dl_tau2([(g.rf.values, g.vf.values) for _, g in F.groupby('km')]); tf_all = 0.0 if not np.isfinite(tf_all) else tf_all
    tf = {}
    for km, g in F.groupby('km'):
        t, Q, d = dl_tau2([(g.rf.values, g.vf.values)])
        tf[km] = t if (np.isfinite(t) and len(g) >= 3) else tf_all
    F['tf'] = F.km.map(tf)
    M = pd.DataFrame([dict(km=k, nm=len(g), nfam=g.kf.nunique(), npromo=g.ID.nunique(),
                           rm=pooled(g.N, g.y, g.vu + tc + tf[k])[0], vm=pooled(g.N, g.y, g.vu + tc + tf[k])[1]) for k, g in u.groupby('km')])
    tm, _, _ = dl_tau2([(M.rm.values, M.vm.values)]); tm = 0.0 if not np.isfinite(tm) else tm
    mu, vmu = pooled(u.N, u.y, u.vu + tc + tf_all + tm)
    F = F.merge(M, on='km')
    def fam_post(r):
        if r.nfam > 1 and r.npromo > 1:
            return shrink(r.rf, r.vf, r.rm, r.vm, r.tf)
        return shrink(r.rf, r.vf, mu, vmu, tm + r.tf)       # единственное семейство механики — к среднему канала
    F[['pf', 'vpf', 'Bf']] = F.apply(lambda r: pd.Series(fam_post(r)), axis=1)
    C = C.merge(F[['kf', 'pf', 'vpf', 'Bf', 'rf', 'ncell', 'nf', 'rm', 'tf', 'nfam']], on='kf')
    C[['post', 'vpost', 'Bc']] = C.apply(lambda r: pd.Series(shrink(r.r, r.v, r.pf, r.vpf, tc) if r.ncell > 1 else (r.pf, r.vpf, 1.0)), axis=1)
    C['sd'] = np.sqrt(C.vpost)
    C['lo'], C['hi'], C['z'] = C.post - 1.96 * C.sd, C.post + 1.96 * C.sd, C.post / C.sd
    par = dict(tau_promo_all=float(np.sqrt(tw_all)), tau_promo={k: round(float(np.sqrt(v)), 2) for k, v in tw.items()},
               tau_cell=float(np.sqrt(tc)), tau_fam_all=float(np.sqrt(tf_all)), tau_fam={k: round(float(np.sqrt(v)), 2) for k, v in tf.items()},
               tau_mech=float(np.sqrt(tm)), mu=float(mu), se_mu=float(np.sqrt(vmu)))
    return C, F, par, u
