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