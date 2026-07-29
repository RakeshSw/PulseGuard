document.querySelectorAll("[data-year]").forEach(function (element) {
  element.textContent = new Date().getFullYear();
});

document.querySelectorAll("[data-copy]").forEach(function (button) {
  button.addEventListener("click", function () {
    var selector = button.getAttribute("data-copy");
    var source = document.querySelector(selector);
    if (!source || !navigator.clipboard) return;

    navigator.clipboard.writeText(source.textContent.trim()).then(function () {
      var original = button.textContent;
      button.textContent = "Copied";
      setTimeout(function () {
        button.textContent = original;
      }, 1400);
    });
  });
});
