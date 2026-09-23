"""Render the architecture Markdown guide as a PDF."""

from pathlib import Path
import re

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    ListFlowable,
    ListItem,
    PageBreak,
    Paragraph,
    Preformatted,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs" / "InsuranceModel_Architecture_Guide.md"
OUTPUT = ROOT / "docs" / "InsuranceModel_Architecture_Guide.pdf"


def inline_markup(text: str) -> str:
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = re.sub(r"`([^`]+)`", r"<font name='Courier'>\1</font>", text)
    text = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", text)
    return text


def footer(canvas, document):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.HexColor("#64748b"))
    canvas.drawString(0.65 * inch, 0.42 * inch, "P&C Insurance Medallion Architecture")
    canvas.drawRightString(7.85 * inch, 0.42 * inch, f"Page {document.page}")
    canvas.restoreState()


def build():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CoverTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=24, leading=30, alignment=TA_CENTER, textColor=colors.HexColor("#12304a"), spaceAfter=18))
    styles.add(ParagraphStyle(name="CoverSub", parent=styles["Normal"], fontSize=12, leading=18, alignment=TA_CENTER, textColor=colors.HexColor("#475569"), spaceAfter=12))
    styles.add(ParagraphStyle(name="H1Custom", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=17, leading=21, textColor=colors.HexColor("#12304a"), spaceBefore=14, spaceAfter=9))
    styles.add(ParagraphStyle(name="H2Custom", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=12.5, leading=16, textColor=colors.HexColor("#0f766e"), spaceBefore=11, spaceAfter=6))
    styles.add(ParagraphStyle(name="BodyCustom", parent=styles["BodyText"], fontSize=9.2, leading=13, spaceAfter=6, textColor=colors.HexColor("#1e293b")))
    styles.add(ParagraphStyle(name="Small", parent=styles["BodyText"], fontSize=8, leading=10, textColor=colors.HexColor("#475569")))

    document = SimpleDocTemplate(str(OUTPUT), pagesize=letter, rightMargin=0.65 * inch, leftMargin=0.65 * inch, topMargin=0.65 * inch, bottomMargin=0.68 * inch, title="P&C Insurance Medallion Architecture")
    story = []
    lines = SOURCE.read_text(encoding="utf-8").splitlines()
    in_code = False
    code_lines = []
    index = 0
    first_heading = True

    while index < len(lines):
        line = lines[index]
        if line.startswith("```"):
            if in_code:
                story.append(Preformatted("\n".join(code_lines), styles["Code"]))
                story.append(Spacer(1, 5))
                code_lines = []
                in_code = False
            else:
                in_code = True
            index += 1
            continue
        if in_code:
            code_lines.append(line)
            index += 1
            continue
        if not line.strip():
            index += 1
            continue
        if line.startswith("# "):
            title = line[2:].strip()
            story.append(Spacer(1, 0.35 * inch) if not first_heading else Spacer(1, 1.2 * inch))
            story.append(Paragraph(inline_markup(title), styles["CoverTitle"]))
            first_heading = False
            index += 1
            continue
        if line.startswith("## "):
            story.append(Paragraph(inline_markup(line[3:].strip()), styles["H1Custom"]))
            index += 1
            continue
        if line.startswith("### "):
            story.append(Paragraph(inline_markup(line[4:].strip()), styles["H2Custom"]))
            index += 1
            continue
        if line.startswith("---"):
            story.append(Spacer(1, 5))
            index += 1
            continue
        if line.startswith("- "):
            items = []
            while index < len(lines) and lines[index].startswith("- "):
                items.append(ListItem(Paragraph(inline_markup(lines[index][2:]), styles["BodyCustom"]), leftIndent=8))
                index += 1
            story.append(ListFlowable(items, bulletType="bullet", leftIndent=16, bulletFontSize=7))
            story.append(Spacer(1, 3))
            continue
        if re.match(r"^\d+\. ", line):
            items = []
            while index < len(lines) and re.match(r"^\d+\. ", lines[index]):
                item_text = re.sub(r"^\d+\. ", "", lines[index])
                items.append(ListItem(Paragraph(inline_markup(item_text), styles["BodyCustom"]), leftIndent=8))
                index += 1
            story.append(ListFlowable(items, bulletType="1", start="1", leftIndent=18))
            story.append(Spacer(1, 3))
            continue
        if line.startswith("|") and index + 1 < len(lines) and lines[index + 1].startswith("|"):
            table_lines = []
            while index < len(lines) and lines[index].startswith("|"):
                if not re.match(r"^\|\s*:?-+:?\s*(\|\s*:?-+:?\s*)+\|?$", lines[index]):
                    cells = [cell.strip() for cell in lines[index].strip().strip("|").split("|")]
                    table_lines.append([Paragraph(inline_markup(cell), styles["Small"]) for cell in cells])
                index += 1
            if table_lines:
                widths = [None] * len(table_lines[0])
                table = Table(table_lines, colWidths=widths, repeatRows=1, hAlign="LEFT")
                table.setStyle(TableStyle([
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#12304a")),
                    ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#cbd5e1")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]))
                story.append(table)
                story.append(Spacer(1, 7))
            continue
        paragraph_lines = [line]
        index += 1
        while index < len(lines) and lines[index].strip() and not re.match(r"^(#|---|[-*] |\d+\. |\||```)", lines[index]):
            paragraph_lines.append(lines[index])
            index += 1
        story.append(Paragraph(inline_markup(" ".join(paragraph_lines)), styles["BodyCustom"]))

    document.build(story, onFirstPage=footer, onLaterPages=footer)
    print(OUTPUT)


if __name__ == "__main__":
    build()
