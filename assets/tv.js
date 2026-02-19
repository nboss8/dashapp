// Fullscreen button (works even when TV page loads dynamically)
document.addEventListener('click', function(e) {
    if (e.target && (e.target.id === 'tv-fullscreen-btn' || e.target.closest('#tv-fullscreen-btn'))) {
        if (!document.fullscreenElement) {
            document.documentElement.requestFullscreen().catch(function() {});
        } else {
            document.exitFullscreen();
        }
    }
});

let hideTimer;
document.addEventListener('mousemove', function() {
    const sb = document.getElementById('tv-sidebar');
    if (sb) sb.style.right = '0px';
    clearTimeout(hideTimer);
    hideTimer = setTimeout(function() {
        const sb = document.getElementById('tv-sidebar');
        if (sb) sb.style.right = '-220px';
    }, 3000);
})