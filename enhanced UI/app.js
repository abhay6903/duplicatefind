const toggleBtn = document.getElementById('themeToggle');
const root = document.documentElement;

// Theme toggle with smooth transition
toggleBtn.addEventListener('click', () => {
  const isDark = root.getAttribute('data-theme') === 'dark';
  const newTheme = isDark ? 'light' : 'dark';
  root.setAttribute('data-theme', newTheme);
  
  try {
    localStorage.setItem('theme', newTheme);
  } catch(e) {
    console.warn('Could not save theme preference');
  }
  
  // Enhanced rotation animation
  toggleBtn.style.transform = 'rotate(360deg) scale(1.2)';
  setTimeout(() => {
    toggleBtn.style.transform = '';
  }, 400);
});

// Load saved theme
try {
  const savedTheme = localStorage.getItem('theme') || 'light';
  root.setAttribute('data-theme', savedTheme);
} catch(e) {
  console.warn('Could not load theme preference');
  root.setAttribute('data-theme', 'light');
}

// Form elements
const connectForm = document.getElementById('connectForm');
const connectStatus = document.getElementById('connectStatus');
const schemaSelect = document.getElementById('schemaSelect');
const schemaInput = document.getElementById('schemaInput');
const tableSelect = document.getElementById('tableSelect');
const tableInput = document.getElementById('tableInput');
const runBtn = document.getElementById('runBtn');
const spinner = document.getElementById('spinner');
const barFill = document.getElementById('barFill');
const pct = document.getElementById('pct');
const runStatus = document.getElementById('runStatus');
const downloadLink = document.getElementById('downloadLink');
const recordForm = document.getElementById('recordForm');
const resetBtn = document.getElementById('resetBtn');
const progressContainer = document.getElementById('progressContainer');
const checkDuplicateBtn = document.getElementById('checkDuplicateBtn');
const schemaBox = document.getElementById('schemaBox');
const tableBox = document.getElementById('tableBox');

let lastJobId = null;
let creds = null;
let jobId = null;
let running = false;
let sessionId = null;

// Enhanced notification system
function showNotification(message, type = 'success') {
  const notification = document.createElement('div');
  notification.className = `notification ${type}`;
  
  // Add icon based on type
  const icon = document.createElement('span');
  if (type === 'success') {
    icon.innerHTML = '✓';
  } else if (type === 'error') {
    icon.innerHTML = '✕';
  } else if (type === 'warning') {
    icon.innerHTML = '⚠';
  }
  icon.style.fontSize = '18px';
  icon.style.fontWeight = 'bold';
  
  notification.appendChild(icon);
  notification.appendChild(document.createTextNode(message));
  document.body.appendChild(notification);
  
  setTimeout(() => {
    notification.style.animation = 'fadeOut 0.3s ease forwards';
    setTimeout(() => notification.remove(), 300);
  }, 3000);
}

function updateStatus(element, message, type = '') {
  element.textContent = message;
  element.className = `status-message ${type}`;
  if(message) element.classList.add('fade-in');
}

function getCredsFromForm() {
  const data = Object.fromEntries(new FormData(connectForm));
  return data;
}

function maybeEnableRun() {
  const schema = schemaSelect.value || schemaInput.value.trim();
  const table = tableSelect.value || tableInput.value.trim();
  const canRun = !!(schema && table);
  
  runBtn.disabled = !canRun;
  runBtn.setAttribute('data-tooltip', canRun ? 'Run deduplication process' : 'Connect and select schema/table first');
  checkDuplicateBtn.disabled = !canRun;
  
  if (canRun) {
    runBtn.classList.remove('loading');
  }
}

// Connection form handler
connectForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  
  const submitBtn = connectForm.querySelector('button');
  submitBtn.classList.add('loading');
  submitBtn.disabled = true;
  
  if (!sessionId) {
    try {
      const sres = await fetch('/session');
      const sjson = await sres.json();
      if (sjson.ok) sessionId = sjson.session_id;
    } catch(e) {
      console.warn('Could not get session ID');
    }
  }
  
  const data = Object.fromEntries(new FormData(connectForm));
  creds = data;
  updateStatus(connectStatus, 'Connecting to database...', 'warning');
  
  try {
    const res = await fetch('/connect', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(data)
    });
    
    const json = await res.json();
    
    if (!json.ok) {
      updateStatus(connectStatus, 'Connection failed: ' + json.error, 'error');
      schemaSelect.innerHTML = '<option value="">— Connection failed —</option>';
      showNotification('Database connection failed', 'error');
      return;
    }
    
    updateStatus(connectStatus, '✓ Connected successfully', 'success');
    schemaBox.classList.add('connected');
    
    schemaSelect.innerHTML = '<option value="">— Select schema —</option>' +
      json.schemas.map(s => `<option value="${s}">${s}</option>`).join('');
    
    showNotification('Database connected successfully!', 'success');
    maybeEnableRun();
    
  } catch (error) {
    updateStatus(connectStatus, 'Connection error: ' + error.message, 'error');
    showNotification('Connection error occurred', 'error');
  } finally {
    submitBtn.classList.remove('loading');
    submitBtn.disabled = false;
  }
});

// Schema selection handler
schemaSelect.addEventListener('change', async () => {
  tableSelect.innerHTML = '<option value="">— Loading tables... —</option>';
  tableBox.classList.remove('connected');
  
  const schema = schemaSelect.value || schemaInput.value;
  if (!schema || !creds) return;
  
  updateStatus(runStatus, 'Loading tables...', 'warning');
  
  try {
    const res = await fetch('/tables', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({...creds, schema})
    });
    
    const json = await res.json();
    
    if (!json.ok) {
      updateStatus(runStatus, 'Error loading tables: ' + json.error, 'error');
      tableSelect.innerHTML = '<option value="">— Error loading tables —</option>';
      return;
    }
    
    updateStatus(runStatus, '', '');
    tableSelect.innerHTML = '<option value="">— Select table —</option>' +
      json.tables.map(t => `<option value="${t}">${t}</option>`).join('');
    
    tableBox.classList.add('connected');
    maybeEnableRun();
    
  } catch (error) {
    updateStatus(runStatus, 'Error: ' + error.message, 'error');
  }
});

schemaInput.addEventListener('input', maybeEnableRun);
tableInput.addEventListener('input', maybeEnableRun);
schemaSelect.addEventListener('change', maybeEnableRun);
tableSelect.addEventListener('change', maybeEnableRun);

// Load columns function
async function loadColumns() {
  const schema = schemaSelect.value || schemaInput.value.trim();
  const table = tableSelect.value || tableInput.value.trim();
  if (!schema || !table) return;
  
  const dataCreds = creds || getCredsFromForm();
  updateStatus(runStatus, 'Loading table columns...', 'warning');
  
  try {
    const res = await fetch('/columns', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({...dataCreds, schema, table})
    });
    
    const json = await res.json();
    if (!json.ok) throw new Error(json.error || 'Failed to load columns');
    
    recordForm.innerHTML = json.columns.map(c => `
      <div class="field">
        <label>${c.name} <span style="color: var(--muted); font-weight: 400;">(${c.type})</span></label>
        <input data-col="${c.name}" placeholder="Enter ${c.name} value" class="styled-input" />
      </div>
    `).join('');
    
    updateStatus(runStatus, `✓ Loaded ${json.columns.length} columns`, 'success');
    setTimeout(() => updateStatus(runStatus, '', ''), 2000);
    
  } catch(err) {
    updateStatus(runStatus, 'Error loading columns: ' + String(err), 'error');
  }
}

schemaSelect.addEventListener('change', loadColumns);
tableSelect.addEventListener('change', loadColumns);
schemaInput.addEventListener('blur', loadColumns);
tableInput.addEventListener('blur', loadColumns);

// Run deduplication
runBtn.addEventListener('click', async () => {
  if (running) return;
  if (!sessionId){
    try{
      const sres = await fetch('/session');
      const sjson = await sres.json();
      if (sjson.ok) sessionId = sjson.session_id;
    }catch{}
  }

  const schema = schemaSelect.value || schemaInput.value.trim();
  const table = tableSelect.value || tableInput.value.trim();

  if (!schema || !table) {
    updateStatus(runStatus, 'Please provide both a schema and a table.', 'error');
    return;
  }
  if (!creds) creds = getCredsFromForm();

  running = true;
  runBtn.disabled = true;
  progressContainer.classList.remove('hidden');
  spinner.classList.remove('hidden');
  barFill.style.width = '0%';
  pct.textContent = '0%';
  updateStatus(runStatus, 'Starting deduplication job...', 'warning');
  downloadLink.classList.add('disabled');

  try {
    const res = await fetch('/run', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({...creds, schema, table, session_id: sessionId})
    });
    const json = await res.json();
    if (!json.ok) throw new Error(json.error);
    
    jobId = json.job_id;
    lastJobId = jobId;
    pollProgress();
  } catch(err) {
    updateStatus(runStatus, 'Error starting job: ' + err.message, 'error');
    running = false;
    runBtn.disabled = false;
    spinner.classList.add('hidden');
  }
});

// Poll progress
async function pollProgress(){
  if (!jobId) return;
  const timer = setInterval(async () => {
    try {
      const res = await fetch(`/progress/${jobId}`);
      const json = await res.json();
      if (!json.ok) { 
        updateStatus(runStatus, 'Error fetching progress: ' + json.error, 'error');
        clearInterval(timer);
        return;
      }
      
      const p = json.progress ?? 0;
      barFill.style.width = `${p}%`;
      pct.textContent = `${p}%`;
      
      if (json.status === 'completed'){
        spinner.classList.add('hidden');
        updateStatus(runStatus, '✓ Deduplication complete!', 'success');
        downloadLink.classList.remove('disabled');
        running = false;
        runBtn.disabled = false;
        clearInterval(timer);
        showNotification('Deduplication job finished!', 'success');
      } else if (json.status === 'error'){
        spinner.classList.add('hidden');
        updateStatus(runStatus, 'Job failed: ' + (json.error || 'unknown error'), 'error');
        running = false;
        runBtn.disabled = false;
        clearInterval(timer);
        showNotification('Deduplication job failed.', 'error');
      } else {
        updateStatus(runStatus, json.status, 'warning');
      }
    } catch(err) {
      updateStatus(runStatus, 'Polling error: ' + err.message, 'error');
      running = false;
      runBtn.disabled = false;
      clearInterval(timer);
    }
  }, 2000);
}

// Check duplicate
checkDuplicateBtn.addEventListener('click', async () => {
  if (!lastJobId){
    updateStatus(runStatus, 'You must run a job before checking a record.', 'warning');
    showNotification('Run a job first', 'warning');
    return;
  }
  
  const record = {};
  let hasValue = false;
  recordForm.querySelectorAll('input[data-col]').forEach(inp => {
    const val = inp.value.trim();
    if(val) {
      record[inp.getAttribute('data-col')] = val;
      hasValue = true;
    }
  });

  if (!hasValue) {
    updateStatus(runStatus, 'Please enter at least one value to check.', 'warning');
    return;
  }
  
  checkDuplicateBtn.classList.add('loading');
  updateStatus(runStatus, 'Checking record...', 'warning');
  
  try {
    const res = await fetch('/check_record', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ job_id: lastJobId, record })
    });
    const json = await res.json();
    if (!json.ok){
      throw new Error(json.error || 'Could not check record');
    }
    
    if (json.result === 'duplicate'){
      let detail = `✓ Result: Duplicate (Cluster ID: ${json.cluster_id})`;
      updateStatus(runStatus, detail, 'success');
      showNotification('Entered Record is a duplicate.', 'success');
    } else if (json.result === 'potential_duplicate') {
      const probPercent = (json.match_probability * 100).toFixed(1);
      let detail = `⚠ Result: Potential Duplicate (Cluster ID: ${json.cluster_id}, Score: ${probPercent}%)`;
      updateStatus(runStatus, detail, 'warning');
      showNotification('Found a potential duplicate for the entered record.', 'warning');
    } else {
      updateStatus(runStatus, '✓ Result: Unique', 'success');
      showNotification('Entered Record appears to be unique.', 'success');
    }
  } catch(err) {
    updateStatus(runStatus, 'Error: ' + err.message, 'error');
  } finally {
    checkDuplicateBtn.classList.remove('loading');
  }
});

// Download report
downloadLink.addEventListener('click', () => {
  if (!lastJobId) {
    showNotification('No job has been run yet.', 'warning');
    return;
  }

  downloadLink.classList.add('loading');
  downloadLink.disabled = true;

  try {
    const url = `/report/${lastJobId}`;
    const a = document.createElement('a');
    a.style.display = 'none';
    a.href = url;
    a.download = 'reports.csv';
    document.body.appendChild(a);
    a.click();
    a.remove();
    showNotification('Report download started.', 'success');
  } catch (err) {
    showNotification(`Failed to download report: ${err.message}`, 'error');
  } finally {
    downloadLink.classList.remove('loading');
    downloadLink.disabled = false;
  }
});

// Reset functionality
resetBtn.addEventListener('click', async () => {
  try{
    await fetch('/reset', { method: 'POST' });
  }catch{}
  
  creds = null; 
  jobId = null; 
  lastJobId = null; 
  running = false; 
  sessionId = null;
  
  connectForm.reset();
  schemaSelect.innerHTML = '<option value="">— Connect to see schemas —</option>';
  tableSelect.innerHTML = '<option value="">— Select schema first —</option>';
  schemaInput.value = '';
  tableInput.value = '';
  
  recordForm.innerHTML = `
    <div class="empty-state">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
        <path d="M3 3h7v7H3zM14 3h7v7h-7zM14 14h7v7h-7zM3 14h7v7H3z"/>
      </svg>
      <p>No table selected</p>
      <span>Connect and select a table to begin</span>
    </div>
  `;
  
  progressContainer.classList.add('hidden');
  spinner.classList.add('hidden');
  barFill.style.width = '0%';
  pct.textContent = '0%';
  
  runBtn.disabled = true;
  checkDuplicateBtn.disabled = true;
  downloadLink.classList.add('disabled');
  
  updateStatus(connectStatus, '', '');
  updateStatus(runStatus, '', '');
  
  schemaBox.classList.remove('connected');
  tableBox.classList.remove('connected');
  
  showNotification('Application has been reset.', 'warning');
});

// Add smooth scroll behavior
document.documentElement.style.scrollBehavior = 'smooth';

// Add parallax effect to gradient orbs
let scrollY = 0;
window.addEventListener('scroll', () => {
  scrollY = window.scrollY;
  const orbs = document.querySelectorAll('.gradient-orb');
  orbs.forEach((orb, index) => {
    const speed = 0.3 + (index * 0.1);
    orb.style.transform = `translateY(${scrollY * speed}px)`;
  });
});

// Enhanced input focus effects
document.querySelectorAll('.styled-input').forEach(input => {
  input.addEventListener('focus', function() {
    this.parentElement.style.transform = 'scale(1.02)';
  });
  
  input.addEventListener('blur', function() {
    this.parentElement.style.transform = 'scale(1)';
  });
});

// Add ripple effect to buttons
document.querySelectorAll('.btn').forEach(button => {
  button.addEventListener('click', function(e) {
    const ripple = document.createElement('span');
    const rect = this.getBoundingClientRect();
    const size = Math.max(rect.width, rect.height);
    const x = e.clientX - rect.left - size / 2;
    const y = e.clientY - rect.top - size / 2;
    
    ripple.style.width = ripple.style.height = size + 'px';
    ripple.style.left = x + 'px';
    ripple.style.top = y + 'px';
    ripple.classList.add('ripple-effect');
    
    this.appendChild(ripple);
    
    setTimeout(() => ripple.remove(), 600);
  });
});

// Add CSS for ripple effect dynamically
const rippleStyle = document.createElement('style');
rippleStyle.textContent = `
  .ripple-effect {
    position: absolute;
    border-radius: 50%;
    background: rgba(255, 255, 255, 0.6);
    transform: scale(0);
    animation: ripple 0.6s ease-out;
    pointer-events: none;
  }
  
  @keyframes ripple {
    to {
      transform: scale(4);
      opacity: 0;
    }
  }
`;
document.head.appendChild(rippleStyle);

// Intersection Observer for animations
const observerOptions = {
  threshold: 0.1,
  rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      entry.target.classList.add('animate-in');
      observer.unobserve(entry.target);
    }
  });
}, observerOptions);

document.querySelectorAll('.card, .workflow-grid > div').forEach(el => {
  observer.observe(el);
});

// Add loading state to form submission
connectForm.addEventListener('submit', function() {
  const submitBtn = this.querySelector('button[type="submit"]');
  submitBtn.style.position = 'relative';
  submitBtn.style.overflow = 'hidden';
});

// Enhanced status message animations
function showStatusWithAnimation(element, message, type) {
  element.style.opacity = '0';
  element.style.transform = 'translateY(-10px)';
  
  setTimeout(() => {
    element.textContent = message;
    element.className = `status-message ${type}`;
    element.style.opacity = '1';
    element.style.transform = 'translateY(0)';
  }, 150);
}

// Keyboard shortcuts
document.addEventListener('keydown', (e) => {
  // Ctrl/Cmd + K to focus search
  if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
    e.preventDefault();
    schemaInput.focus();
  }
  
  // Escape to clear focus
  if (e.key === 'Escape') {
    document.activeElement.blur();
  }
});

// Add hover effect to cards
document.querySelectorAll('.card').forEach(card => {
  card.addEventListener('mouseenter', function() {
    this.style.transition = 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)';
  });
});

// Progress bar animation enhancement
function animateProgress(targetWidth) {
  const currentWidth = parseFloat(barFill.style.width) || 0;
  const increment = (targetWidth - currentWidth) / 20;
  let current = currentWidth;
  
  const animation = setInterval(() => {
    current += increment;
    if ((increment > 0 && current >= targetWidth) || (increment < 0 && current <= targetWidth)) {
      current = targetWidth;
      clearInterval(animation);
    }
    barFill.style.width = `${current}%`;
    pct.textContent = `${Math.round(current)}%`;
  }, 50);
}

// Add focus visible for accessibility
document.querySelectorAll('button, input, select').forEach(element => {
  element.addEventListener('focus', function() {
    this.style.outline = 'none';
  });
});

// Dynamic greeting based on time
function getGreeting() {
  const hour = new Date().getHours();
  if (hour < 12) return 'Good morning';
  if (hour < 18) return 'Good afternoon';
  return 'Good evening';
}

// Add active state to buttons
document.querySelectorAll('.btn').forEach(btn => {
  btn.addEventListener('mousedown', function() {
    this.style.transform = 'scale(0.95)';
  });
  
  btn.addEventListener('mouseup', function() {
    this.style.transform = '';
  });
  
  btn.addEventListener('mouseleave', function() {
    this.style.transform = '';
  });
});

// Prevent double submissions
let isSubmitting = false;
connectForm.addEventListener('submit', function(e) {
  if (isSubmitting) {
    e.preventDefault();
    return false;
  }
});

runBtn.addEventListener('click', function() {
  if (isSubmitting) return;
  isSubmitting = true;
  setTimeout(() => {
    isSubmitting = false;
  }, 2000);
});

// Auto-save form state (except passwords)
function saveFormState() {
  try {
    const formData = {
      host: connectForm.querySelector('[name="host"]').value,
      port: connectForm.querySelector('[name="port"]').value,
      user: connectForm.querySelector('[name="user"]').value,
      catalog: connectForm.querySelector('[name="catalog"]').value
    };
    localStorage.setItem('formState', JSON.stringify(formData));
  } catch(e) {
    console.warn('Could not save form state');
  }
}

function loadFormState() {
  try {
    const savedState = localStorage.getItem('formState');
    if (savedState) {
      const formData = JSON.parse(savedState);
      if (formData.host) connectForm.querySelector('[name="host"]').value = formData.host;
      if (formData.port) connectForm.querySelector('[name="port"]').value = formData.port;
      if (formData.user) connectForm.querySelector('[name="user"]').value = formData.user;
      if (formData.catalog) connectForm.querySelector('[name="catalog"]').value = formData.catalog;
    }
  } catch(e) {
    console.warn('Could not load form state');
  }
}

// Load form state on page load
window.addEventListener('load', loadFormState);

// Save form state on input
connectForm.querySelectorAll('input').forEach(input => {
  input.addEventListener('change', saveFormState);
});

console.log('%c Zigma Dedupe - UI Loaded', 'color: #0f7b74; font-size: 16px; font-weight: bold;');
