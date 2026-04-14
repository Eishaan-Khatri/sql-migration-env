/* =====================================================
   SQL Migration Agent Benchmark — Interactivity Engine
   ===================================================== */

document.addEventListener('DOMContentLoaded', () => {

    // ─── Scroll Reveal Animation ─────────────────────
    const revealElements = document.querySelectorAll('.reveal');
    const revealObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
            }
        });
    }, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

    revealElements.forEach(el => revealObserver.observe(el));

    // ─── Animated Counter ────────────────────────────
    const counters = document.querySelectorAll('[data-count]');
    const counterObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const el = entry.target;
                const target = el.getAttribute('data-count');
                animateCounter(el, target);
                counterObserver.unobserve(el);
            }
        });
    }, { threshold: 0.5 });

    counters.forEach(el => counterObserver.observe(el));

    function animateCounter(el, target) {
        const isDecimal = target.includes('.');
        const end = parseFloat(target);
        const duration = 1500;
        const start = performance.now();

        function update(now) {
            const elapsed = now - start;
            const progress = Math.min(elapsed / duration, 1);
            const eased = 1 - Math.pow(1 - progress, 3);
            const current = eased * end;

            if (isDecimal) {
                el.textContent = current.toFixed(target.split('.')[1].length);
            } else {
                el.textContent = Math.floor(current).toLocaleString();
            }

            if (progress < 1) requestAnimationFrame(update);
        }
        requestAnimationFrame(update);
    }

    // ─── Score Bar Animation ─────────────────────────
    const scoreBars = document.querySelectorAll('.score-bar');
    const barObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const bar = entry.target;
                const width = bar.getAttribute('data-width');
                bar.style.width = width + '%';
                barObserver.unobserve(bar);
            }
        });
    }, { threshold: 0.3 });

    scoreBars.forEach(bar => {
        bar.style.width = '0%';
        barObserver.observe(bar);
    });

    // ─── Copy to Clipboard ───────────────────────────
    document.querySelectorAll('.copy-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const code = btn.closest('.code-block').querySelector('pre').textContent;
            navigator.clipboard.writeText(code).then(() => {
                const original = btn.textContent;
                btn.textContent = 'Copied!';
                btn.style.color = '#3fb950';
                setTimeout(() => {
                    btn.textContent = original;
                    btn.style.color = '';
                }, 2000);
            });
        });
    });

    // ─── Docs Tabs ───────────────────────────────────
    document.querySelectorAll('.docs-tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const panelId = tab.getAttribute('data-panel');

            document.querySelectorAll('.docs-tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.docs-panel').forEach(p => p.classList.remove('active'));

            tab.classList.add('active');
            document.getElementById(panelId).classList.add('active');
        });
    });

    // ─── Try It Live Demo ────────────────────────────
    const tryItBtn = document.getElementById('try-it-btn');
    const tryItSelect = document.getElementById('try-it-select');
    const tryItOutput = document.getElementById('try-it-output');
    const tryItStatus = document.getElementById('try-it-status');

    if (tryItBtn) {
        tryItBtn.addEventListener('click', async () => {
            const task = tryItSelect.value;
            const baseUrl = 'https://Eishaan-sql-migration-env.hf.space';

            // Set loading state
            tryItStatus.className = 'try-it-status loading';
            tryItStatus.innerHTML = '<span class="spinner"></span> Calling API...';
            tryItOutput.textContent = 'Sending POST /reset to HF Space...\n';
            tryItBtn.disabled = true;

            try {
                const response = await fetch(`${baseUrl}/reset`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ task_name: task }),
                });

                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();

                tryItStatus.className = 'try-it-status success';
                tryItStatus.textContent = '● Response received';

                // Pretty print the response
                const pretty = JSON.stringify(data, null, 2);
                tryItOutput.textContent = pretty;
            } catch (err) {
                tryItStatus.className = 'try-it-status error';
                tryItStatus.textContent = '● Error';
                tryItOutput.textContent =
                    `// Could not reach HF Space.\n` +
                    `// The space may be sleeping or rebuilding.\n` +
                    `// Error: ${err.message}\n\n` +
                    `// You can try the API directly:\n` +
                    `curl -X POST https://Eishaan-sql-migration-env.hf.space/reset \\\n` +
                    `  -H "Content-Type: application/json" \\\n` +
                    `  -d '{"task_name": "${task}"}'`;
            } finally {
                tryItBtn.disabled = false;
            }
        });
    }

    // ─── Smooth Scroll for Nav Links ─────────────────
    document.querySelectorAll('a[href^="#"]').forEach(link => {
        link.addEventListener('click', e => {
            e.preventDefault();
            const target = document.querySelector(link.getAttribute('href'));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        });
    });

    // ─── Navbar scroll effect ────────────────────────
    const navbar = document.querySelector('.navbar');
    window.addEventListener('scroll', () => {
        if (window.scrollY > 50) {
            navbar.style.borderBottomColor = 'rgba(255,255,255,0.12)';
        } else {
            navbar.style.borderBottomColor = 'rgba(255,255,255,0.08)';
        }
    });

});
