from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from pathlib import Path

def model_picker_kbd(ctx, cwd, models, step=1, provider=None):
    """
    Devuelve texto y teclado para seleccionar modelo.
    step=1: muestra proveedores
    step=2: muestra modelos del proveedor
    """
    if step == 1:
        # Agrupar por proveedor
        groups = {}
        for m in models:
            pid = m.get("providerID", "?")
            groups.setdefault(pid, []).append(m)
        btns = [
            [InlineKeyboardButton(f"{pid}", callback_data=f"pickmodel:{_key(ctx, pid)}")] for pid in sorted(groups)
        ]
        btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
        txt = f"📂 `{Path(cwd).name}`\nSelecciona proveedor:"
        return txt, InlineKeyboardMarkup(btns)
    else:
        # Mostrar modelos del proveedor
        mids = sorted([m.get("id") or m.get("modelID", "?") for m in models if m.get("providerID") == provider])
        btns = []
        row = []
        for mid in mids:
            mk = _key(ctx, f"{provider}|{mid}")
            row.append(InlineKeyboardButton(mid, callback_data=f"pickmodelset:{mk}"))
            if len(row) == 2:
                btns.append(row); row = []
        if row:
            btns.append(row)
        btns.append([InlineKeyboardButton("⚙ Modelo por defecto", callback_data="pickmodelset:default")])
        btns.append([InlineKeyboardButton("⬅ Volver", callback_data="pickmodel:back")])
        btns.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel:")])
        txt = f"📂 `{Path(cwd).name}`\nModelos de *{provider}*"
        return txt, InlineKeyboardMarkup(btns)

def _key(ctx, value: str) -> int:
    store = ctx.bot_data.setdefault("ks", {})
    for k, v in store.items():
        if v == value:
            return k
    k = len(store)
    store[k] = value
    return k
