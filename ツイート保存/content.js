// content.js

let isAutoScrolling = false;
let scrollInterval = null;
const SCROLL_SPEED_MS = 2500; 
let scrollAmountPx = 500; 

// ページをリロードするまでの間、一時的に処理済みIDを記憶しておくセット
const processedTweetIds = new Set();
// 同じページ内で同じ画像URLが複数回登場した場合の重複リクエスト防止
const processedUrls = new Set();

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
  if (request.command === "start") {
    if (request.scrollAmount) scrollAmountPx = parseInt(request.scrollAmount, 10);
    stopScrolling();
    startScrolling();
  } else if (request.command === "stop") {
    stopScrolling();
  }
});

function startScrolling() {
  if (isAutoScrolling) return;
  isAutoScrolling = true;
  console.log(`Twitter Saver: Started (Scroll: ${scrollAmountPx}px)`);
  scanTweets();
  
  scrollInterval = setInterval(() => {
    // behavior: 'auto' で負荷を下げつつ確実に移動
    window.scrollBy({ top: scrollAmountPx, behavior: 'auto' });
    setTimeout(scanTweets, 1000); 
  }, SCROLL_SPEED_MS);
}

function stopScrolling() {
  if (scrollInterval) {
    clearInterval(scrollInterval);
    scrollInterval = null;
  }
  isAutoScrolling = false;
  console.log("Twitter Saver: Stopped");
}

function scanTweets() {
  if (!isAutoScrolling) return;

  const tweets = document.querySelectorAll('.tweet, article');

  tweets.forEach((element) => {
    let tweetId = element.getAttribute('data-tweet-id');
    if (!tweetId) {
      const link = element.querySelector('a[href*="/status/"]');
      if (link) {
        const match = link.getAttribute('href').match(/status\/(\d+)/);
        if (match) tweetId = match[1];
      }
    }

    // 既にこのセッションで見たツイートは即スキップ（DOM解析コスト削減）
    if (!tweetId || processedTweetIds.has(tweetId)) return;

    let username = "user";
    const handleSpan = element.querySelector('.tweet-header-handle');
    if (handleSpan) {
      username = handleSpan.innerText.replace('@', '').trim();
    } else {
      const avatarLink = element.querySelector('.tweet-avatar-link, a[href^="/"]');
      if (avatarLink) {
        const href = avatarLink.getAttribute('href');
        if (href.startsWith('/')) username = href.substring(1).split('/')[0];
      }
    }

    // ファイル名に使える文字だけ残す処理
    const textDiv = element.querySelector('.tweet-body-text, div[data-testid="tweetText"]');
    let tweetText = textDiv ? textDiv.innerText : "";
    const safeText = tweetText.replace(/[\\/:*?"<>|]/g, '_').replace(/\s+/g, '').substring(0, 15);

    // 画像処理
    const imgs = Array.from(element.querySelectorAll('img')).filter(img => 
      !img.classList.contains('tweet-avatar') && 
      !img.classList.contains('emoji') && 
      (img.src.includes('pbs.twimg.com/media') || img.closest('.tweet-post-image'))
    );

    imgs.forEach((img, index) => {
      let src = img.src;
      try {
        const urlObj = new URL(src);
        urlObj.searchParams.set('name', 'orig');
        src = urlObj.toString();
      } catch(e) {}
      
      // ファイル名の生成 (一意性を高める)
      const filename = `twitter_save/${username}_${tweetId}_img${index}_${safeText}.jpg`;
      downloadItem(src, filename);
    });

    // 動画処理
    const videos = element.querySelectorAll('video');
    videos.forEach((video, index) => {
      let videoUrl = video.src;
      if (!videoUrl) {
        const source = video.querySelector('source');
        if (source) videoUrl = source.src;
      }
      if (videoUrl && !videoUrl.startsWith('blob:') && videoUrl.includes('http')) {
         if (videoUrl.includes('.m3u8')) return;
         const filename = `twitter_save/${username}_${tweetId}_mov${index}_${safeText}.mp4`;
         downloadItem(videoUrl, filename);
      }
    });

    // JSON保存 (IDベースでファイル名を作る)
    const jsonFilename = `twitter_save/${username}_${tweetId}.json`;
    // JSONのデータ生成処理（重いので必要なければコメントアウトも可）
    let timestamp = null;
    const timeEl = element.querySelector('.tweet-time');
    if (timeEl) {
       const dataTs = timeEl.getAttribute('data-timestamp');
       if (dataTs) timestamp = new Date(parseInt(dataTs)).toISOString();
    }
    
    // JSON保存のリクエストも投げるが、background側で同名ファイルがあれば無視される
    chrome.runtime.sendMessage({
      type: "save_json",
      data: {
        tweet_id: tweetId,
        user_name: username,
        text: tweetText,
        saved_at: new Date().toISOString()
      },
      filename: jsonFilename
    });

    processedTweetIds.add(tweetId);
  });
}

function downloadItem(url, filename) {
  // セッション内での重複チェック（連打防止）
  if (processedUrls.has(url)) return;
  
  chrome.runtime.sendMessage({
    type: "download_image",
    url: url,
    filename: filename
  });
  
  processedUrls.add(url);
}