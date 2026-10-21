# Template for the Homebrew tap. The release job fills in VERSION and each SHA256_* placeholder
# from the .sha256 file published beside the matching tarball.
class Mitosis < Formula
  desc "Instant copy-on-write Postgres clones"
  homepage "https://github.com/irfanfaraaz/mitosis"
  version "VERSION"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "https://github.com/irfanfaraaz/mitosis/releases/download/vVERSION/mitosis-vVERSION-aarch64-apple-darwin.tar.gz"
      sha256 "SHA256_AARCH64_APPLE_DARWIN"
    end
    on_intel do
      url "https://github.com/irfanfaraaz/mitosis/releases/download/vVERSION/mitosis-vVERSION-x86_64-apple-darwin.tar.gz"
      sha256 "SHA256_X86_64_APPLE_DARWIN"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/irfanfaraaz/mitosis/releases/download/vVERSION/mitosis-vVERSION-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "SHA256_AARCH64_UNKNOWN_LINUX_GNU"
    end
    on_intel do
      url "https://github.com/irfanfaraaz/mitosis/releases/download/vVERSION/mitosis-vVERSION-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "SHA256_X86_64_UNKNOWN_LINUX_GNU"
    end
  end

  def install
    bin.install "mitosis"
  end

  test do
    system "#{bin}/mitosis", "--version"
  end
end
