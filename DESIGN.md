# Local workspace design

Approved direction: restrained, evidence-first operating interface. System sans-serif typography, neutral surfaces, blue actions, clear processing boundaries. No images, decorative infrastructure widgets, or invented runtime statistics.

Tokens: spacing 8/16/24/32/40/48/64px; body 16px; section headings 20px; page heading 32px (28px narrow); radius 4px. Background #f5f6f8; surface #ffffff; ink #172333; secondary #536173; border #dce2e9; action #204dc5; success #176346; error #9b2633.

Structure: header with product and local mode; concise purpose; primary upload/result workspace plus secondary dataset contract. Narrow screens collapse in reading order. Upload/result surfaces own padding and borders. Buttons and links use consistent roles; no ornamental cards.

Accessibility: real labels/buttons, visible keyboard focus, textual states, role=status for processing and success, role=alert for failures, no fake percentages or motion. Controls are at least 44px high. No external fonts, scripts, or style dependencies.

Current interface claims: reference processing/count reconciliation and a separate local durable job lifecycle. Arbitrary uploaded data is not independently verified. Mode labels distinguish reference and durable emulation; neither claims AWS validation. Durable pages show committed counts, stable job links, reconnection/cancellation uncertainty and absolute expiry. Download targets remain at least 44px, counts stack at narrow widths, and polling updates existing status without moving focus.
