#!/usr/bin/env python

import qrcode

class QRImage(qrcode.QRCode):
    def __init__(self, text=None, fit=True, version=None,
                    error_correction=qrcode.constants.ERROR_CORRECT_M,
                    box_size=10, border=4, image_factory=None):
        self.qr = qrcode.QRCode(version=version, error_correction=error_correction, box_size=box_size, border=border)
        self.qr.add_data(text)
        self.qr.make(fit=fit)
        self.image = self.qr.make_image()

    def save_image(self,filename='qr.png'):
        self.image.save(filename)

    def print_qr_ascii(self):
        self.qr.print_ascii()

if __name__ == "__main__":
    qr = QRImage(text="helloworld")
    qr.save_image()
    qr.print_qr_ascii()


