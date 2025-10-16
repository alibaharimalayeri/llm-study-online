import time
import functools
from datetime import datetime
import streamlit as st
import pandas as pd

# =========================
# Retry helper (for 429s)
# =========================
def retry(backoffs=(0.5, 1.0, 2.0, 4.0)):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last = None
            for t in (0.0, *backoffs):
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    last = e
                    if t > 0:
                        time.sleep(t)
            raise last
        return wrapper
    return deco


# =========================
# Google Sheets helpers
# =========================
@st.cache_resource
def get_ws():
    """Authorize once and return the 'results' worksheet (create + seed header if needed)."""
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(st.secrets["SHEET_ID"])
    try:
        ws = sh.worksheet("results")
    except Exception:
        ws = sh.add_worksheet(title="results", rows=1, cols=20)
        # header
        ws.update(
            "A1:K1",
            [[
                "ts_iso","participant","q_index","qid","question",
                "model_answer","accuracy","completeness","usefulness",
                "style_tone","comment"
            ]],
        )
    return ws


@st.cache_data(ttl=30)
@retry()
def get_answered_indices(name: str) -> set:
    """
    Return the set of q_index values already answered by this participant.
    Reads only B:C (participant, q_index) to reduce quota.
    """
    if not name:
        return set()
    ws = get_ws()
    rows = ws.get_values("B2:C")  # list of [participant, q_index]
    target = name.strip().lower()
    answered = set()
    for r in rows:
        if len(r) >= 2 and r[0] and r[1]:
            if str(r[0]).strip().lower() == target:
                try:
                    answered.add(int(float(r[1])))
                except ValueError:
                    pass
    return answered


@retry()
def append_result(row_dict: dict):
    ws = get_ws()
    ws.append_row(
        [
            row_dict["ts_iso"],
            row_dict["participant"],
            row_dict["q_index"],
            row_dict["qid"],
            row_dict["question"],
            row_dict["model_answer"],
            row_dict["accuracy"],
            row_dict["completeness"],
            row_dict["usefulness"],
            row_dict["style_tone"],
            row_dict["comment"],
        ],
        value_input_option="RAW",
    )


# =========================
# Local CSV loader
# =========================
@st.cache_data
def load_questions():
    df = pd.read_csv("questions.csv")
    # Expected columns: qid, question, answer/model_answer
    cols = {c.lower(): c for c in df.columns}

    def first_key(*opts):
        for o in opts:
            if o in cols:
                return cols[o]
        return None

    qid_col = first_key("qid", "id")
    q_col = first_key("question", "prompt")
    a_col = first_key("answer", "model_answer", "response")
    if not all([qid_col, q_col, a_col]):
        raise ValueError("questions.csv must have columns: qid, question, answer/model_answer")

    df = df.rename(columns={qid_col: "qid", q_col: "question", a_col: "model_answer"})
    df["q_index"] = range(1, len(df) + 1)
    return df[["q_index", "qid", "question", "model_answer"]]


# =========================
# UI
# =========================
st.set_page_config(page_title="LLM Answer Evaluation", layout="wide")

st.sidebar.header("Study Info")
name = st.sidebar.text_input("Name (required)", value=st.session_state.get("name", "")).strip()
st.session_state["name"] = name

qs = load_questions()
st.sidebar.caption(f"Questions loaded: **{len(qs)}**")
with st.sidebar:
    st.info("Your ratings are saved to a secure Google Sheet.\n\n"
            "Resume anytime by entering the **same name**.", icon="💾")

st.title("LLM Answer Evaluation")

if not name:
    st.warning("Enter your **Name** in the left sidebar to begin.")
    st.markdown("""
### Please read before starting
- **Auto-save & resume:** Your ratings auto-save to a secure Google Sheet. To continue later, reopen this page and enter the **same name**.
- **Privacy:** Only the fields you submit (name, ratings, optional comment) are stored with timestamps.
- **Send us your results:** You can also download your personal CSV at the end and email it as backup.
    """)
    st.stop()

# Progress / resume
answered = get_answered_indices(name)
remaining = sorted(set(qs["q_index"]) - answered)
st.progress(len(answered) / len(qs))
st.caption(f"Progress: **{len(answered)} / {len(qs)}**")

if not remaining:
    st.success("✅ You’ve completed all questions. Thank you!")
    # Optional: allow participant to download their rows
    try:
        ws = get_ws()
        all_rows = ws.get_all_records()
        df_all = pd.DataFrame(all_rows)
        mine = df_all[df_all["participant"].str.lower() == name.lower()] if not df_all.empty else pd.DataFrame()
        if not mine.empty:
            st.download_button(
                "Download my results (CSV)",
                mine.to_csv(index=False).encode("utf-8"),
                file_name=f"results_{name}.csv",
            )
    except Exception:
        pass
    st.stop()

next_idx = remaining[0]
row = qs.set_index("q_index", drop=False).loc[next_idx]

st.subheader(f"Question {int(row['q_index'])} of {len(qs)}")
st.write("**QID:**", row["qid"])
st.markdown("**Question:** " + row["question"])
st.markdown("**Model Answer:**")
st.info(row["model_answer"])

st.divider()

# Neutral-by-default sliders
choices = ["—", 1, 2, 3, 4, 5]
acc = st.select_slider("Accuracy", options=choices, value="—")
comp = st.select_slider("Completeness", options=choices, value="—")
use = st.select_slider("Usefulness", options=choices, value="—")
style = st.select_slider("Style/Tone", options=choices, value="—")
comment = st.text_area("Optional comment", placeholder="(optional)")

col1, col2 = st.columns(2)
with col1:
    if st.button("💾 Save & Next", type="primary"):
        missing = [k for k, v in {
            "Accuracy": acc, "Completeness": comp, "Usefulness": use, "Style/Tone": style
        }.items() if v == "—"]
        if missing:
            st.error("Please rate: " + ", ".join(missing))
            st.stop()
        try:
            append_result({
                "ts_iso": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "participant": name,
                "q_index": int(row["q_index"]),   # safe because drop=False above
                "qid": str(row["qid"]),
                "question": row["question"],
                "model_answer": row["model_answer"],
                "accuracy": acc,
                "completeness": comp,
                "usefulness": use,
                "style_tone": style,
                "comment": comment.strip(),
            })
            st.success("Saved!")
            time.sleep(0.3)
            st.cache_data.clear()  # <-- add this line
            st.rerun()
        except Exception as e:
            st.exception(e)

with col2:
    # Optional personal download at any time (best-effort, ignore errors)
    try:
        ws = get_ws()
        all_rows = ws.get_all_records()
        df_all = pd.DataFrame(all_rows)
        mine = df_all[df_all["participant"].str.lower() == name.lower()] if not df_all.empty else pd.DataFrame()
        if not mine.empty:
            st.download_button(
                "Download my results (CSV)",
                mine.to_csv(index=False).encode("utf-8"),
                file_name=f"results_{name}.csv",
            )
    except Exception:
        pass
