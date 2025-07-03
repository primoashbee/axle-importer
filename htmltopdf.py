from weasyprint import HTML
import os

html_folder = 'sendgrid_templates/HTMLS'
output_file = 'combined.pdf'

# 🗂️ Get all HTML files sorted (optional)
html_files = sorted([
    os.path.join(html_folder, f)
    for f in os.listdir(html_folder)
    if f.lower().endswith('.html')
])

parts = []
for file_path in html_files:
    filename = os.path.basename(file_path)
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Add filename header and wrap content in a div with page break after
    part = f"""
    <div style="page-break-after: always;">
    <p style="text-align: center; margin-bottom: 20px; font-size: 10px;">{filename}</p>
      {content}
    </div>
    """
    parts.append(part)

full_html = f"<html><body>{''.join(parts)}</body></html>"

HTML(string=full_html).write_pdf('combined_with_filenames.pdf')
print("PDF generated: combined_with_filenames.pdf")